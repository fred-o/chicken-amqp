;; -*- geiser-scheme-implementation: 'chicken; -*-

(module amqp-primitives *

  (import scheme
		  (chicken base)
		  (chicken condition)
		  (chicken syntax)
		  (chicken random)
          bitstring
		  srfi-18
          amqp-core
          amqp-091)

  ;; AMQP primitives API

  (define-record amqp-message delivery properties payload)

  (define amqp-payload-conversion (make-parameter bitstring->blob))

  (define-syntax with-locked
	(syntax-rules ()
	  ((_ channel body ...)
	   (let [(lock (channel-lock channel))]
		 (handle-exceptions exp
			 (begin
			   (mutex-unlock! lock)
			   (signal exp))
		   (mutex-lock! lock)
		   (let [(res (begin body ...))]
			 (mutex-unlock! lock)
			 res))))))

  (define (amqp-receive-message channel)
	(with-locked channel
				 (let* [(mthd (expect-frame channel 60 '(50 60 71) accessor: channel-content-mbox))
						(hdrs (read-frame channel accessor: channel-content-mbox))
						(body-size (* 8 (frame-body-size hdrs)))
						(buf (string->bitstring ""))]
				   (let loop []
					 (when (< (bitstring-length buf) body-size)
					   (set! buf (bitstring-append! buf
													(frame-payload (read-frame channel accessor: channel-content-mbox))))
					   (loop)))
				   (make-amqp-message
					(frame-properties mthd)
					(frame-properties hdrs)
					(if (amqp-payload-conversion)
						((amqp-payload-conversion) buf)
						buf)))))

  (define (amqp-publish-message channel exchange routing-key payload properties #!key (mandatory 0) (immediate 0))
	(let [(pl (->bitstring payload))]
	  (with-locked channel
				   (let [(frame-max (- (alist-ref 'frame-max (connection-parameters (channel-connection channel))) 8))
						 (payload-length (/ (bitstring-length pl) 8))]
					 (write-frame channel 1 (make-basic-publish exchange routing-key mandatory immediate))
					 (write-frame channel 2 (encode-headers-payload 60 0 payload-length properties))
					 (let loop ((from 0) (to frame-max))
					   (write-frame channel 3 (bitstring-share pl (* from 8) (* (min to payload-length) 8)))
					   (when (< to payload-length)
						 (loop (+ from frame-max) (+ to frame-max))))
					 (void)))))

  (define (amqp-channel-open connection)
	(let ((channel (new-channel connection)))
	  (write-frame channel 1 (make-channel-open))
	  (expect-frame channel 20 11)
	  channel))

  (define-syntax define-amqp-operation
	(er-macro-transformer
	 (lambda (exp inject compare)
	   (define (applied-args ls)
		 (cond ((null? ls) ls)
			   ((eq? '#!key (car ls)) (applied-args (cdr ls)))
			   ((list? (car ls)) (cons (caar ls) (applied-args (cdr ls))))
			   (else (cons (car ls) (applied-args (cdr ls))))))
	   (let* ([name (cadr exp)]
			  [args (caddr exp)]
			  [expect (cdddr exp)]
			  [has-no-wait? (member '(no-wait 0) args)]
			  [op (string->symbol (string-append "amqp-" (symbol->string name)))]
			  [fn (string->symbol (string-append "make-" (symbol->string name)))])
		 `(define (,op ,@(cons 'channel args))
			(with-locked channel
						 (write-frame channel 1 (,fn ,@(applied-args args)))
						 ,(cond ((null? expect) `(void))
								(has-no-wait?
								 `(if (= no-wait 1) (void)
									  (frame-properties (expect-frame channel ,@expect))))
								(else
								 `(frame-properties (expect-frame channel ,@expect))))))))))

  (define-amqp-operation channel-flow (active) 20 21)
  (define-amqp-operation channel-close (#!key (reply-code 0) (reply-text "") (method-id 0) (class-id 0)) 20 41)

  (define-amqp-operation exchange-declare (exchange type #!key (passive 0) (durable 0) (no-wait 0) (arguments '())) 40 11)
  (define-amqp-operation exchange-delete (exchange #!key (if-unused 0) (no-wait 0)) 40 21)
  
  (define-amqp-operation queue-declare (queue #!key (passive 0) (durable 0) (exclusive 0) (auto-delete 0) (no-wait 0) (arguments '())) 50 11)
  (define-amqp-operation queue-bind (queue exchange routing-key #!key (no-wait 0) (arguments '())) 50 21)
  (define-amqp-operation queue-purge (queue  #!key (no-wait 0)) 50 31)
  (define-amqp-operation queue-delete (queue #!key (if-unused 0) (if-empty 0) (no-wait 0)) 50 41)
  (define-amqp-operation queue-unbind (queue exchange routing-key #!key (arguments '())) 50 51)
  
  (define-amqp-operation basic-qos (prefetch-size prefetch-count global) 60 11)
  (define-amqp-operation basic-consume (queue #!key (consumer-tag "") (no-local 0) (no-ack 0) (exclusive 0) (no-wait 0) (arguments '())) 60 21)
  (define-amqp-operation basic-cancel (consumer-tag #!key (no-wait 0)) 60 31)
  (define-amqp-operation basic-get (queue #!key (no-ack 0)) 60 '(71 72))
  (define-amqp-operation basic-ack (delivery-tag #!key (multiple 0)))
  (define-amqp-operation basic-reject (delivery-tag #!key (requeue 0)))
  (define-amqp-operation basic-recover (requeue) 60 111)
  (define-amqp-operation basic-recover-async (requeue))

  (define-amqp-operation tx-select () 90 11)
  (define-amqp-operation tx-commit () 90 21)
  (define-amqp-operation tx-rollback () 90 31))
