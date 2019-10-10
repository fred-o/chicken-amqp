(import ssax sxpath (chicken pretty-print) srfi-1)

(define amqp-xml-spec '())

(call-with-input-file "amqp0-9-1.stripped.xml"
  (lambda (port)
    (set! amqp-xml-spec (ssax:xml->sxml port '()))
    (close-input-port port)))

(define (spec-prop prop node)
  (let ((ret (alist-ref prop (alist-ref '@ (cdr node)))))
    (if ret (car ret) ret)))

(define (parser-name class-name method-name)
  (string->symbol (string-append "amqp:parse-" class-name "-" method-name)))

(define (make-parser class-name method)
  (let ((method-name (spec-prop 'name method)))
    `(define (,(parser-name class-name method-name) bits)
       (bitmatch
        bits
        (,(foldl append '()
                 (map (lambda (arg)
                        (let* ((field-name (spec-prop 'name arg))
                               (field-name-symbol (string->symbol field-name))
                               (size-symbol (string->symbol (string-append field-name "-size")))
                               (domain (spec-prop 'domain arg)))
                          (cond
                           ((equal? "octet" domain) `((,field-name-symbol 8)))
                           ((equal? "longstr" domain)
                            `((,size-symbol 32) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                           ((equal? "peer-properties" domain)
                            `((,size-symbol 32) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                           (else '(())))))
                      ((sxpath '(field)) method)))
         (list ,class-name ,method-name
               (list ,@(map (lambda (arg)
                       (let* ((field-name (spec-prop 'name arg))
                              (field-name-symbol (string->symbol field-name))
                              (domain (spec-prop 'domain arg)))
                         `(list ',field-name-symbol
                               ,(cond
                                ((equal? "octet" domain) field-name-symbol)
                                ((equal? "longstr" domain) `(bitstring->string ,field-name-symbol))
                                ((equal? "peer-properties" domain) `(parse-table ,field-name-symbol))
                                (else #f)))))
                     ((sxpath '(field)) method)))))))))

(for-each pp
          (foldl append '()
                 (map
                  (lambda (cls)
                    (let ((class-name (spec-prop 'name cls)))
                      (map (lambda (method) (make-parser class-name method))
                           ((sxpath '(method)) cls))))
                  ((sxpath `(// class)) amqp-xml-spec))))

(define (make-method-dispatch-clause class-name method)
  (let ((method-name (spec-prop 'name method))
        (method-index (string->number (spec-prop 'index method))))
    `((eq? ,method-index method-id)
      (,(parser-name class-name method-name) arguments))))

(define (make-class-dispatch-clause cls)
  (let ((class-name (spec-prop 'name cls))
        (class-index (string->number (spec-prop 'index cls))))
    `((eq? ,class-index class-id)
      (cond ,@(map (lambda (method)
                     (make-method-dispatch-clause class-name method))
                   ((sxpath '(method)) cls))))))

(define (make-class-dispatch classes)
  `(cond ,@(map make-class-dispatch-clause classes)))

(pp `(define (parse-method bits)
       (bitmatch
        bits
        (((class-id 16)
          (method-id 16)
          (arguments bitstring))
         ,(make-class-dispatch ((sxpath `(// class)) amqp-xml-spec))))))

