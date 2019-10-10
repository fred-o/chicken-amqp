(import ssax sxpath (chicken pretty-print))

(define amqp-xml-spec '())

(call-with-input-file "amqp0-9-1.stripped.xml"
  (lambda (port)
    (set! amqp-xml-spec (ssax:xml->sxml port '()))
    (close-input-port port)))


;; (define (spec-prop name node) (car (alist-ref name (cdar ((sxpath `(@)) node)))))

(define (spec-prop prop node) (car (alist-ref prop (alist-ref '@ (cdr node)))))

(define (make-method-dispatch-clause class-name method)
  (let ((method-name (spec-prop 'name method))
        (method-index (spec-prop 'index method)))
    `((eq ,method-index method-id)
      (,(string->symbol (string-append "parse-" class-name "-" method-name))))))

(define (make-class-dispatch-clause cls)
  (let ((class-name (spec-prop 'name cls))
        (class-index (spec-prop 'index cls)))
    `((eq ,class-index class-id)
      (cond ,@(map (lambda (method)
                     (make-method-dispatch-clause class-name method))
                   ((sxpath '(method)) cls))))))

(define (make-class-dispatch classes)
  `(cond ,@(map make-class-dispatch-clause classes)))

(pp (make-class-dispatch ((sxpath `(// class)) amqp-xml-spec)))
;; (for-each make-class-dispatch ((sxpath `(// class)) amqp-xml-spec))
