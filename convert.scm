(import ssax sxpath (chicken pretty-print) srfi-1)

(define amqp-xml-spec '())

(call-with-input-file "amqp0-9-1.stripped.xml"
  (lambda (port)
    (set! amqp-xml-spec (ssax:xml->sxml port '()))
    (close-input-port port)))

(define (spec-prop prop node)
  (let ((ret (alist-ref prop (alist-ref '@ (cdr node)))))
    (if ret (car ret) ret)))


(define (maker-name class-name method-name)
  (string->symbol (string-append "amqp:make-" class-name "-" method-name)))

(define (make-maker class-id class-name method)
  (let ((method-id (spec-prop 'index method))
        (method-name (spec-prop 'name method)))
    `(define (,(maker-name class-name method-name)
              ,@(map (lambda (arg) (string->symbol (spec-prop 'name arg)))
                     ((sxpath '(field)) method)))
       (bitconstruct
        (,(string->number class-id) 16)
        (,(string->number method-id) 16)
        ,@(foldl append '()
               (map (lambda (arg)
                      (let* ((field-name (spec-prop 'name arg))
                             (field-name-symbol (string->symbol field-name))
                             (domain  (or (spec-prop 'domain arg)
                                          (spec-prop 'type arg))))
                        (cond
                         ((equal? "bit" domain) `((,field-name-symbol 8)))
                         ((equal? "long" domain) `((,field-name-symbol 32)))
                         ((equal? "longstr" domain) `(((string-length ,field-name-symbol) 32) (,field-name-symbol bitstring)))
                         ((equal? "path" domain) `(((string-length ,field-name-symbol) 8) (,field-name-symbol bitstring)))
                         ((equal? "peer-properties" domain) '((0 32)))
                         ((equal? "short" domain) `((,field-name-symbol 16)))
                         ((equal? "shortstr" domain) `(((string-length ,field-name-symbol) 8) (,field-name-symbol bitstring)))
                         (else '(())))))
                    ((sxpath '(field)) method)))))))

(for-each (lambda (form)
            (pp form)
            (print))
          (foldl append '()
                 (map
                  (lambda (cls)
                    (let ((class-id (spec-prop 'index cls))
                          (class-name (spec-prop 'name cls)))
                      (map (lambda (method) (make-maker class-id class-name method))
                           ((sxpath "method[chassis/@name = 'server']") cls))))
                  ((sxpath `(// class)) amqp-xml-spec))))


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
                               (domain (or (spec-prop 'domain arg)
                                           (spec-prop 'type arg))))
                          (cond
                           ((equal? "bit" domain) `((,field-name-symbol 8)))
                           ((equal? "long" domain) `((,field-name-symbol 32)))
                           ((equal? "longstr" domain) `((,size-symbol 32) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                           ((equal? "octet" domain) `((,field-name-symbol 8)))
                           ((equal? "path" domain) `((,size-symbol 8) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                           ((equal? "peer-properties" domain) `((,size-symbol 32) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                           ((equal? "short" domain) `((,field-name-symbol 16)))
                           ((equal? "shortstr" domain) `((,size-symbol 8) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                           (else '(())))))
                      ((sxpath '(field)) method)))
         (list ,class-name ,method-name
               (list ,@(map (lambda (arg)
                       (let* ((field-name (spec-prop 'name arg))
                              (field-name-symbol (string->symbol field-name))
                              (domain (or (spec-prop 'domain arg)
                                          (spec-prop 'type arg))))
                         `(list ',field-name-symbol
                               ,(cond
                                 ((equal? "bit" domain) field-name-symbol)
                                 ((equal? "long" domain) field-name-symbol)
                                 ((equal? "longstr" domain) `(bitstring->string ,field-name-symbol))
                                 ((equal? "octet" domain) field-name-symbol)
                                 ((equal? "path" domain) `(bitstring->string ,field-name-symbol))
                                 ((equal? "peer-properties" domain) `(parse-table ,field-name-symbol))
                                 ((equal? "short" domain) field-name-symbol)
                                 ((equal? "shortstr" domain) `(bitstring->string ,field-name-symbol))
                                 (else #f)))))
                            ((sxpath '(field)) method)))))))))

(for-each (lambda (form)
            (pp form)
            (print))
          (foldl append '()
                 (map
                  (lambda (cls)
                    (let ((class-name (spec-prop 'name cls)))
                      (map (lambda (method) (make-parser class-name method))
                           ((sxpath "method[chassis/@name = 'client']") cls))))
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
                   ((sxpath "method[chassis/@name = 'client']") cls))))))

(define (make-class-dispatch classes)
  `(cond ,@(map make-class-dispatch-clause classes)))

(pp `(define (parse-method bits)
       (bitmatch
        bits
        (((class-id 16)
          (method-id 16)
          (arguments bitstring))
         (print "class-id " class-id " method-id " method-id)
         ,(make-class-dispatch ((sxpath `(// class)) amqp-xml-spec))))))
