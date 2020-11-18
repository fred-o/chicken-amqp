;; -*- geiser-scheme-implementation: 'chicken -*-

(module amqp-core *
  
  (import scheme
		  (chicken base)
		  (chicken syntax)
		  (chicken tcp)
		  (chicken io)
		  (chicken string)
          (chicken process-context)
		  (chicken format)
		  (chicken condition)
		  amqp-091
		  srfi-18
		  mailbox
		  bitstring
		  uri-generic)
  
  (define *amqp-header*
	(bitstring->string (bitconstruct
						("AMQP" bitstring)
						(#d0 8)
						(0 8) (9 8) (1 8))))

  (define amqp-debug (make-parameter (substring-index "amqp" (or (get-environment-variable "DEBUG") ""))))
  
  (define (print-debug #!rest args) (when (amqp-debug) (apply print args)))

  (define-record connection in out lock mboxes parameters closing)

  (define-record channel connection method-mbox content-mbox id lock)
  
  ;; Initialize a new channel object and register its mailboxes with
  ;; the connection.
  (define (new-channel conn)
	(let ((lock (connection-lock conn)))
	  (mutex-lock! lock)
	  (let* [(channel-id (+ 1 (foldl max -1 (map car (connection-mboxes conn)))))
			 (method-mbox (make-mailbox))
			 (content-mbox (make-mailbox))
			 (channel (make-channel conn method-mbox content-mbox channel-id (make-mutex)))]
		(connection-mboxes-set! conn
								(cons (cons channel-id (cons method-mbox content-mbox))
									  (connection-mboxes conn)))
		(mutex-unlock! lock)
		channel)))

  ;; Send an AMQP frame over the wire, thread safe and blocking
  (define (write-frame channel type payload)
	(let [(conn (channel-connection channel))
		  (ch (channel-id channel))]
	  (if (port-closed? (connection-out conn)) (error "connection closed"))
	  (let ([lock (connection-lock conn)]
			[frm (encode-frame type ch payload)])
		(print-debug " <-- channel:" ch " type:" type " payload:" payload)
		(mutex-lock! lock)
		(write-string (bitstring->string frm)
					  #f
					  (connection-out conn))
		(mutex-unlock! lock))))

  ;; Read the next frame on this channel, thread safe and blocking.
  (define (read-frame channel #!key (accessor channel-method-mbox))
	(let* [(mbox (accessor channel))]
	  (let ((frm (mailbox-receive! mbox)))
		(cond
		 ((condition? frm)
		  (mailbox-push-back! mbox frm)
		  (raise frm))
		 (else frm)))))

  ;; Like read-frame, but throw an error if the frame is not of the
  ;; expected class and method
  (define (expect-frame channel class-id method-id #!key (accessor channel-method-mbox))
	(let* [(frm (read-frame channel accessor: accessor))]
	  (cond
	   ((frame-match? frm #f class-id method-id)
		frm)
	   ((frame-match? frm #f 20 40)
		(let ((props (frame-properties frm)))
		  (raise (condition '(exn message "channel closed")
							`(amqp reply-text ,(alist-ref 'reply-text props)
								   reply-code ,(alist-ref 'reply-code props))))))
	   (else 
		(error (format "expected class: ~a (got ~a), method: ~a (got ~a)"
					   class-id (frame-class-id frm)
					   method-id (frame-method-id frm)))))))
  
  (define (dispatcher connection)
	(lambda ()
	  (let* ((lock (connection-lock connection))
			(in (connection-in connection))
			(buf (->bitstring ""))
			(result (call/cc
					 (lambda (break)
					   (let loop ()
						 (handle-exceptions exp (break (if (connection-closing connection)
														   (condition '(exn message "connection closed"))
														   exp))
						   (let ((first-byte (read-string 1 in)))
							 (if (eq? #!eof first-byte)
								 (break (condition '(exn message "connection closed unexpectedly")))
								 (begin
								   (bitstring-append! buf (string->bitstring (string-append first-byte (read-buffered in))))
								   (let parse-loop ()
									 (let* ((message/rest (parse-frame buf))
											(frm (car message/rest)))
									   (set! buf (cdr message/rest))
									   (when frm
										 (print-debug " --> " frm)
										 (mutex-lock! lock)
										 (let ((mbox-pair (alist-ref (frame-channel frm)
																	 (connection-mboxes connection))))
										   (mutex-unlock! lock)
										   (if mbox-pair
											   (begin
												 ;; Dispatch to method-mbox / content-mbox depending on frame type.
												 (mailbox-send! (if (or (frame-match? frm '(2 3) #f #f)
																		(frame-match? frm 1 60 '(50 60 71)))
																	(cdr mbox-pair)
																	(car mbox-pair)) frm)
												 ;; A special case: basic.get-ok is dispatched to both the method and
												 ;; content mbox as it is both a method response and a message preamble
												 (when (frame-match? frm 1 60 71)
												   (mailbox-send! (car mbox-pair) frm)))
											   (error "no mailbox for channel")))
										 ;; Check for connection close
										 (if (frame-match? frm 1 10 50)
											 (let ((props (frame-properties frm)))
											   (break (condition '(exn message "connection closed")
																 `(amqp reply-text ,(alist-ref 'reply-text props)
																		reply-code ,(alist-ref 'reply-code props))))))
										 (parse-loop)))))))
						   (loop)))))))
		(print-debug "dispatcher closing")
		;; Time to poison the well
		(mutex-lock! lock)
		(for-each (lambda (mbox) (mailbox-send! mbox result))
				  (map cadr (connection-mboxes connection)))
		(mutex-unlock! lock))))

  (define (heartbeats channel)
	(lambda ()
      (let* [(conn (channel-connection channel))
			 (interval (/ (alist-ref 'heartbeat (connection-parameters conn)) 2))
             (payload (string->bitstring ""))]
		(let loop ()
		  (unless (port-closed? (connection-out conn))
			(write-frame channel 8 payload)
			(sleep interval)
			(loop)))
		(print-debug "heartbeats closing"))))

  ;; Create a new AMQP connection with an initial channel with id 0
  (define (amqp-connect url)
	(let* [(uri (uri-reference url))
		   (host (uri-host uri))
		   (port (or (uri-port uri) 5672))
		   (username (or (uri-username uri) ""))
		   (password (or (uri-password uri) ""))
		   (vhost (apply string-append
						 (map (lambda (x) (if (symbol? x) (symbol->string x) x))
							  (uri-path uri))))]
	  (unless (eq? 'amqp (uri-scheme uri)) (error "not an AMQP uri"))
	  (let-values (((i o) (tcp-connect host port)))
		(let* ((conn (make-connection i o (make-mutex) '() '() #f))
			   (channel (new-channel conn))
			   (dt (make-thread (dispatcher conn) "dispatcher")))
		  (thread-start! dt)
		  ;; Initiate handshake
		  (write-string *amqp-header* #f o)
		  ;; Authorization
		  (let ((frm (expect-frame channel 10 10)))
			(write-frame channel 1
                         (make-connection-start-ok  '(("connection_name" . "Chicken AMQP"))
                                                    "PLAIN"
                                                    (string-append "\x00" username "\x00" password)
                                                    "en_US")))
		  ;; Negotiation
		  (let* ((frm (expect-frame channel 10 30))
				 (args (frame-properties frm)))
            (connection-parameters-set! conn args)
            (write-frame channel 1 (make-connection-tune-ok (alist-ref 'channel-max args)
														   (alist-ref 'frame-max args)
														   (alist-ref 'heartbeat args)))
            (write-frame channel 1 (make-connection-open vhost)))
		  ;; Connection is open
		  (expect-frame channel 10 41)
		  ;; Start a heartbeat thread
		  (let ((hb (make-thread (heartbeats channel) "heartbeats")))
			(thread-start! hb))
		  conn))))

  (define (amqp-disconnect conn)
	(let ((lock (connection-lock conn)))
	  (mutex-lock! lock)
	  (connection-closing-set! conn #t)
	  (close-output-port (connection-out conn))
	  (close-input-port (connection-in conn))
	  (mutex-unlock! lock))))
