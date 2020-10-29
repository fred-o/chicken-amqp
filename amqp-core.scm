;; -*- geiser-scheme-implementation: 'chicken -*-

(module amqp-core *
  
  (import scheme (chicken base) (chicken syntax)
		  (chicken tcp)
		  (chicken io)
		  (chicken irregex)
          (chicken process-context)
		  (chicken format)
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

  (define-record connection in out thread lock mboxes parameters state)

  (define-record channel connection id mbox)

  ;; Initialize a new channel object and register the mailbox with the
  ;; connection. 
  (define (new-channel conn)
	(let* ((next-id (+ 1 (foldl max -1 (map car (connection-mboxes conn)))))
		   (ch (make-channel conn next-id (make-mailbox))))
	  (connection-mboxes-set! conn
							  (cons (cons next-id (channel-mbox ch))
									(connection-mboxes conn)))
	  ch))

  ;; Send an AMQP frame over the wire, thread safe and blocking
  (define (write-frame conn ch type payload)
	(let ([lock (connection-lock conn)])
	  (print-debug " <-- channel:" channel " type:" type " payload:" payload)
	  (mutex-lock! lock)
	  (write-string (bitstring->string (encode-frame type ch payload))
					#f
					(connection-out conn))
	  (mutex-unlock! lock)))

  ;; Read the next frame on this channel, thread safe and blocking.
  (define (read-frame conn ch)
	(let ((mbox (alist-ref ch (connection-mboxes conn))))
	  (mailbox-receive! mbox)))

  ;; Like read-frame, but throw an error if the frame is not of the
  ;; expected class and method
  (define (expect-frame conn ch class-id method-id)
	(let ((frm (read-frame conn ch)))
	  (if (and (= (frame-class-id frm) class-id)
			   (= (frame-method-id frm) method-id))
		  frm
		  (error (format "expected class: ~a (got ~a), method: ~a (got ~a)"
						 class-id (frame-class-id frm)
						 method-id (frame-method-id frm))))))
  
  (define (dispatcher connection)
	(lambda ()
      (tcp-read-timeout #f)
	  (call/cc
	   (lambda (break)
		 (letrec ((in (connection-in connection))
				  (buf (->bitstring ""))
				  (loop (lambda ()
						  (let ((first-byte (read-string 1 in)))
							(if (eq? #!eof first-byte)
								(break)
								(begin
								  (bitstring-append! buf (string->bitstring (string-append first-byte (read-buffered in))))
								  (letrec [(parse-loop (lambda ()
														 (let* ((message/rest (parse-frame buf))
																(frm (car message/rest)))
														   (set! buf (cdr message/rest))
														   (when frm
															 (print-debug "dispatching: " frm)
															 (let ((mbox (alist-ref (frame-channel frm)
																					(connection-mboxes connection))))
															   (if mbox (mailbox-send! mbox frm)
																   (error "no mailbox for channel")))
															 (parse-loop)))))]
									(parse-loop))))
							(loop)))))
		   (loop))))))

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
		  (let* ((conn (make-connection i o #f (make-mutex) '() '() #:new))
				 (ch (new-channel conn))
				 (dt (make-thread (dispatcher conn))))
			(connection-thread-set! conn dt)
			(thread-start! dt)
			;; Initiate handshake
			(write-string *amqp-header* #f o)
			;; Authorization
			(let ((frm (expect-frame conn 0 10 10)))
			  (write-frame conn 0 1
                           (make-connection-start-ok  '(("connection_name" . "Chicken AMQP"))
                                                      "PLAIN"
                                                      (string-append "\x00" username "\x00" password)
                                                      "en_US"))

			  (print frm))
			;; Negotiation
			(let* ((frm (expect-frame conn 0 10 30))
				   (args (frame-properties frm)))
			  (print frm)
              (connection-parameters-set! conn args)
              (write-frame conn 0 1 (make-connection-tune-ok (alist-ref 'channel-max args)
															 (alist-ref 'frame-max args)
															 (alist-ref 'heartbeat args)))
              (write-frame conn 0 1 (make-connection-open vhost)))
			;; Connection is open
			(expect-frame conn 0 10 41)
			(connection-state-set! conn #:open)
			conn))))

	(define (amqp-disconnect conn)
	  (let ((lock (connection-lock conn)))
		(mutex-lock! lock)
		(close-output-port (connection-out conn))
		(close-input-port (connection-in conn))
		(mutex-unlock! lock)))
	
	)

(import amqp-core)
(print (connection-parameters (amqp-connect "amqp://local:panda4ever@localhost/")))

