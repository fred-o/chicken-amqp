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

  (define is-debug (irregex-match? '(: "amqp") (or (get-environment-variable "DEBUG") "")))
  
  (define (print-debug #!rest args) (when is-debug (apply print args)))

  (define-record connection in out threads lock mboxes parameters)

  (define-record channel connection id mbox)

  ;; Initialize a new channel object and register the mailbox with the
  ;; connection. 
  (define (new-channel conn)
	(let* ((channel-id (+ 1 (foldl max -1 (map car (connection-mboxes conn))))))
	  (connection-mboxes-set! conn
							  (cons (cons channel-id (make-mailbox))
									(connection-mboxes conn)))
	  channel-id))

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
	(if (port-closed? (connection-in conn)) (error "connection closed"))
	(let ((mbox (alist-ref ch (connection-mboxes conn))))
	  (mailbox-receive! mbox)))

  ;; Like read-frame, but throw an error if the frame is not of the
  ;; expected class and method
  (define (expect-frame conn ch class-id method-id)
	(let ((frm (read-frame conn ch)))
	  (if (and (= (frame-class-id frm) class-id)
			   (= (frame-method-id frm) method-id))
		  frm
		  (if (and (= 20 (frame-class-id frm))
				   (= 40 (frame-method-id frm)))
			  (error "channel closed")
			  (error (format "expected class: ~a (got ~a), method: ~a (got ~a)"
							 class-id (frame-class-id frm)
							 method-id (frame-method-id frm)))))))
  
  (define (dispatcher connection)
	(lambda ()
	  (call/cc
	   (lambda (break)
		 (letrec ((in (connection-in connection))
				  (buf (->bitstring ""))
				  (loop (lambda ()
						  (with-exception-handler (lambda (e)
													(print e)
													(break e))
							(let ((first-byte (read-string 1 in)))
							  (if (eq? #!eof first-byte)
								  (break #!eof)
								  (begin
									(bitstring-append! buf (string->bitstring (string-append first-byte (read-buffered in))))
									(letrec [(parse-loop (lambda ()
														   (let* ((message/rest (parse-frame buf))
																  (frm (car message/rest)))
															 (set! buf (cdr message/rest))
															 (when frm
															   (print-debug " --> " frm)
															   (let ((mbox (alist-ref (frame-channel frm)
																					  (connection-mboxes connection))))
																 (if mbox (mailbox-send! mbox frm)
																	 (error "no mailbox for channel")))
															   ;; Check for connection close
															   (if (and (= 10 (frame-class-id frm))
																		(= 50 (frame-method-id frm)))
																   (break '()))
															   (parse-loop)))))]
									  (parse-loop))))
							  (loop))))))
		   (loop))))
	  (print-debug "dispatcher closing")))

  (define (heartbeats conn)
	(lambda ()
      (letrec ((interval (/ (alist-ref 'heartbeat (connection-parameters conn)) 2))
               (payload (string->bitstring ""))
               (loop (lambda ()
                       (write-frame conn 0 8 payload)
                       (sleep interval)
                       (loop))))
		(loop))))
  
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
		(let* ((conn (make-connection i o '() (make-mutex) '() '()))
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
			(thread-start! hb)
			(connection-threads-set! conn (list dt hb)))
		  conn))))

  (define (amqp-disconnect conn)
	(let ((lock (connection-lock conn)))
	  (mutex-lock! lock)
	  (close-output-port (connection-out conn))
	  (close-input-port (connection-in conn))
	  (mutex-unlock! lock)))
  )
