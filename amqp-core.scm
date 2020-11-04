;; -*- geiser-scheme-implementation: 'chicken -*-

(module amqp-core *
  
  (import scheme (chicken base) (chicken syntax)
		  (chicken tcp)
		  (chicken io)
		  (chicken irregex)
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

  (define amqp-debug (irregex-match? '(: "amqp") (or (get-environment-variable "DEBUG") "")))
  
  (define (print-debug #!rest args) (when amqp-debug (apply print args)))

  (define-record connection in out lock mboxes parameters)

  (define-record channel connection id mbox)

  ;; Initialize a new channel object and register the mailbox with the
  ;; connection. 
  (define (new-channel conn)
	(let ((lock (connection-lock conn)))
	  (mutex-lock! lock)
	  (let* ((channel-id (+ 1 (foldl max -1 (map car (connection-mboxes conn))))))
		(connection-mboxes-set! conn
								(cons (cons channel-id (make-mailbox))
									  (connection-mboxes conn)))
		(mutex-unlock! lock)
		channel-id)))

  ;; Send an AMQP frame over the wire, thread safe and blocking
  (define (write-frame conn ch type payload)
	(if (port-closed? (connection-out conn)) (error "connection closed"))
	(let ([lock (connection-lock conn)]
		  [frm (encode-frame type ch payload)])
	  (print-debug " <-- channel:" ch " type:" type " payload:" payload)
	  (mutex-lock! lock)
	  (write-string (bitstring->string frm)
					#f
					(connection-out conn))
	  (mutex-unlock! lock)))

  ;; Read the next frame on this channel, thread safe and blocking.
  (define (read-frame conn ch)
	(let* ((mbox (alist-ref ch (connection-mboxes conn)))
		   (frm (mailbox-receive! mbox)))
	  (cond
	   ((condition? frm)
		(mailbox-push-back! mbox frm)
		(raise frm))
	   (else frm))))

  ;; Like read-frame, but throw an error if the frame is not of the
  ;; expected class and method
  (define (expect-frame conn ch class-id method-id)
	(let* ((frm (read-frame conn ch)))
	  (cond
	   ((and (= (frame-class-id frm) class-id)
			 (= (frame-method-id frm) method-id))
		frm)
	   ((and (= 20 (frame-class-id frm))
			 (= 40 (frame-method-id frm)))
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
						 (handle-exceptions exp (break exp)
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
										 (let ((mbox (alist-ref (frame-channel frm)
																(connection-mboxes connection))))
										   (mutex-unlock! lock)
										   (if mbox
											   (mailbox-send! mbox frm)
											   (error "no mailbox for channel")))
										 ;; Check for connection close
										 (if (and (= 10 (frame-class-id frm))
												  (= 50 (frame-method-id frm)))
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
				  (map cdr (connection-mboxes connection)))
		(mutex-unlock! lock))))

  (define (heartbeats conn)
	(lambda ()
      (let ((interval (/ (alist-ref 'heartbeat (connection-parameters conn)) 10))
            (payload (string->bitstring "")))
		(handle-exceptions exp (lambda i i)
		  (let loop ()
			(write-frame conn 0 8 payload)
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
		(let* ((conn (make-connection i o (make-mutex) '() '()))
			   (ch (new-channel conn))
			   (dt (make-thread (dispatcher conn) "dispatcher")))
		  (thread-start! dt)
		  ;; Initiate handshake
		  (write-string *amqp-header* #f o)
		  ;; Authorization
		  (let ((frm (expect-frame conn 0 10 10)))
			(write-frame conn 0 1
                         (make-connection-start-ok  '(("connection_name" . "Chicken AMQP"))
                                                    "PLAIN"
                                                    (string-append "\x00" username "\x00" password)
                                                    "en_US")))
		  ;; Negotiation
		  (let* ((frm (expect-frame conn 0 10 30))
				 (args (frame-properties frm)))
            (connection-parameters-set! conn args)
            (write-frame conn 0 1 (make-connection-tune-ok (alist-ref 'channel-max args)
														   (alist-ref 'frame-max args)
														   (alist-ref 'heartbeat args)))
            (write-frame conn 0 1 (make-connection-open vhost)))
		  ;; Connection is open
		  (expect-frame conn 0 10 41)
		  ;; Start a heartbeat thread
		  (let ((hb (make-thread (heartbeats conn) "heartbeats")))
			(thread-start! hb))
		  conn))))

  (define (amqp-disconnect conn)
	(let ((lock (connection-lock conn)))
	  (mutex-lock! lock)
	  (close-output-port (connection-out conn))
	  (close-input-port (connection-in conn))
	  (mutex-unlock! lock))))
