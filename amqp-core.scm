;; -*- geiser-scheme-implementation: 'chicken -*-

(module amqp-core *

  (import scheme (chicken base) (chicken syntax)
		  (chicken tcp)
		  (chicken io)
		  (chicken irregex)
          (chicken process-context)
		  amqp-091
		  srfi-18
		  mailbox
		  bitstring
		  uri-generic)

  (define is-debug (irregex-match? '(: "amqp") (or (get-environment-variable "DEBUG") "")))
  
  (define (print-debug #!rest args) (when is-debug (apply print args)))

  (define-record connection in out thread lock mboxes)

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
  (define (write-frame ch frm)
	(let ([conn (channel-connection ch)])
	  (mutex-lock! (connection-lock conn))
	  (write-string "" #f (connection-out conn))
	  (mutex-unlock! (connection-lock conn))))

  ;; Read the next frame on this channel, thread safe and blocking.
  (define (read-frame ch)
	(let ((mbox (alist-ref (channel-id ch)
						   (connection-mboxes (channel-connection ch)))))
	  (mailbox-receive! mbox)))


  (define (dispatcher connection)
	(lambda ()
      (tcp-read-timeout #f)
      (letrec ((in (connection-in connection))
               (buf (->bitstring ""))
               (loop (lambda ()
                       (let ((first-byte (read-string 1 in)))
						 (if (eq? #!eof first-byte)
							 (print "Eof")
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
      (define-values (i o) (tcp-connect host port))
	  (let* ((conn (make-connection i o #f (make-mutex) '()))
			 (ch (new-channel conn))
			 (dt (make-thread (dispatcher conn))))
		(connection-thread-set! conn dt)
		conn)))

  )
