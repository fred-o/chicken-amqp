(import (chicken pretty-print))

(with-output-to-file "./docs/amqp.scm"
  (lambda ()
	(pp `(module amqp *
	   ,@(map (lambda (def)
			   `(define (,(string->symbol (string-append "amqp-" (symbol->string (cadr def))))
						 channel
						 ,@(caddr def))
				  ()))
			 '((define-amqp-operation channel-flow (active) 20 21)
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
			   (define-amqp-operation tx-rollback () 90 31)))))))
