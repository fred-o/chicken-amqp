(import ssax sxpath (chicken pretty-print) srfi-1)

(define amqp-xml-spec '())

(call-with-input-file "amqp0-9-1.stripped.xml"
  (lambda (port)
    (set! amqp-xml-spec (ssax:xml->sxml port '()))
    (close-input-port port)))

(define (spec-prop prop node)
  (let ((ret (alist-ref prop (alist-ref '@ (cdr node)))))
    (if ret (car ret) ret)))

(define (pack-bitfields fields)
  (drop-right
   (cdr (foldl (lambda (acc field)
                 (if (eq? 1 (cadr field))
                     (cons (cons field (car acc)) (cdr acc))
                     (let ((len (length (car acc))))
                       (if (> len 0)
                           (cons '() (append  (cdr acc) `((0 ,(- 8 len))) (car acc) (list field)))
                           (cons '() (append (cdr acc) (list field)))))))
               '(() . ()) (append fields '((#f 0)))))
   1))

(define (maker-name class-name method-name)
  (string->symbol (string-append "amqp:make-" class-name "-" method-name)))

(define (make-maker class-id class-name method)
  (let ((method-id (spec-prop 'index method))
        (method-name (spec-prop 'name method))
        (encodables ((sxpath "field[@domain = 'peer-properties' or @domain = 'table']") method)))
    `(define (,(maker-name class-name method-name)
              ,@(map (lambda (arg) (string->symbol (spec-prop 'name arg)))
                     ((sxpath "field[not(@reserved = '1')]") method)))
       ,(let ((variables (map (lambda (field)
                                (let ((encodable-symbol (string->symbol (spec-prop 'name field)))
                                      (encoded-encodable-symbol (string->symbol (string-append "encoded-" (spec-prop 'name field)))))
                                  `(,encoded-encodable-symbol (encode-table ,encodable-symbol)))) encodables))
              (body
               `(bitconstruct
                 (,(string->number class-id) 16)
                 (,(string->number method-id) 16)
                 ,@(pack-bitfields
                    (foldl append '()
                           (map (lambda (arg)
                                  (let* ((field-name (spec-prop 'name arg))
                                         (field-name-symbol (string->symbol field-name))
                                         (encoded-field-name-symbol (string->symbol (string-append "encoded-" field-name)))
                                         (reserved (equal? (spec-prop 'reserved arg) "1"))
                                         (domain  (or (spec-prop 'domain arg)
                                                      (spec-prop 'type arg))))
                                    (cond
                                     ((equal? "longstr" domain) (if reserved `((0 32)) `(((string-length ,field-name-symbol) 32) (,field-name-symbol bitstring))))
                                     ((or (equal? "peer-properties" domain)
                                          (equal? "table" domain)) `(((/ (bitstring-length ,encoded-field-name-symbol) 8) 32) (,encoded-field-name-symbol bitstring)))
                                     ((equal? "octet" domain) `((,(if reserved 0 field-name-symbol) 1)))
                                     ((or (equal? "bit" domain)
                                          (equal? "no-ack" domain)
                                          (equal? "no-local" domain)
                                          (equal? "no-wait" domain)
                                          (equal? "redelivered" domain)) `((,(if reserved 0 field-name-symbol) 1)))
                                     ((or (equal? "long" domain)
                                          (equal? "message-count" domain)) `((,field-name-symbol 32)))
                                     ((or (equal? "longlong" domain)
                                          (equal? "delivery-tag" domain)) `((,field-name-symbol 64)))
                                     ((or (equal? "short" domain)
                                          (equal? "class-id" domain)
                                          (equal? "method-id" domain)
                                          (equal? "reply-code" domain)) `((,(if reserved 0 field-name-symbol) 16)))
                                     ((or (equal? "shortstr" domain)
                                          (equal? "consumer-tag" domain)
                                          (equal? "exchange-name" domain)
                                          (equal? "path" domain)
                                          (equal? "queue-name" domain)
                                          (equal? "reply-text" domain)) (if reserved `((0 8)) `(((string-length ,field-name-symbol) 8) (,field-name-symbol bitstring))))
                                     (else '(())))))
                                ((sxpath '(field)) method)))))))
          (if (null? variables)
              body
              `(let ,variables ,body))))))


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

(define (make-parser class-id class-name method)
  (let ((method-id (spec-prop 'index method))
        (method-name (spec-prop 'name method)))
    `(define (,(parser-name class-name method-name) bits)
       (bitmatch
        bits
        (,(pack-bitfields
           (foldl append '()
                  (map (lambda (arg)
                         (let* ((field-name (spec-prop 'name arg))
                                (field-name-symbol (string->symbol field-name))
                                (size-symbol (string->symbol (string-append field-name "-size")))
                                (domain (or (spec-prop 'domain arg)
                                            (spec-prop 'type arg))))
                           (cond
                            ((or (equal? "long" domain)
                                 (equal? "message-count" domain)) `((,field-name-symbol 32)))
                            ((or (equal? "longlong" domain)
                                 (equal? "delivery-tag" domain)) `((,field-name-symbol 64)))
                            ((equal? "longstr" domain) `((,size-symbol 32) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                            ((or (equal? "peer-properties" domain)
                                 (equal? "table" domain)) `((,size-symbol 32) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                            ((equal? "octet" domain) `((,field-name-symbol 8)))
                            ((or (equal? "bit" domain)
                                 (equal? "no-ack" domain)
                                 (equal? "no-local" domain)
                                 (equal? "no-wait" domain)
                                 (equal? "redelivered" domain)) `((,field-name-symbol 1)))
                            ((or (equal? "short" domain)
                                 (equal? "class-id" domain)
                                 (equal? "method-id" domain)
                                 (equal? "reply-code" domain)) `((,field-name-symbol 16)))
                            ((or (equal? "shortstr" domain)
                                 (equal? "consumer-tag" domain)
                                 (equal? "exchange-name" domain)
                                 (equal? "path" domain)
                                 (equal? "queue-name" domain)
                                 (equal? "reply-text" domain)) `((,size-symbol 8) (,field-name-symbol (* ,size-symbol 8) bitstring)))
                            (else '(())))))
                       ((sxpath '(field)) method))))
         (list ,(string->number class-id) ,(string->number method-id)
               (list ,@(map (lambda (arg)
                              (let* ((field-name (spec-prop 'name arg))
                                     (field-name-symbol (string->symbol field-name))
                                     (domain (or (spec-prop 'domain arg)
                                                 (spec-prop 'type arg))))
                                `(cons ',field-name-symbol
                                       ,(cond
                                         ((or (equal? "long" domain)
                                              (equal? "message-count" domain)) field-name-symbol)
                                         ((or (equal? "longlong" domain)
                                              (equal? "delivery-tag" domain)) field-name-symbol)
                                         ((equal? "longstr" domain) `(bitstring->string ,field-name-symbol))
                                         ((or (equal? "octet" domain)
                                              (equal? "bit" domain)
                                              (equal? "no-ack" domain)
                                              (equal? "no-local" domain)
                                              (equal? "no-wait" domain)
                                              (equal? "redelivered" domain)) field-name-symbol)
                                         ((or (equal? "peer-properties" domain)
                                              (equal? "table" domain)) `(parse-table ,field-name-symbol))
                                         ((or (equal? "short" domain)
                                              (equal? "class-id" domain)
                                              (equal? "method-id" domain)
                                              (equal? "reply-code" domain)) field-name-symbol)
                                         ((or (equal? "shortstr" domain)
                                              (equal? "consumer-tag" domain)
                                              (equal? "exchange-name" domain)
                                              (equal? "path" domain)
                                              (equal? "queue-name" domain)
                                              (equal? "reply-text" domain)) `(bitstring->string ,field-name-symbol))
                                         (else #f)))))
                            ((sxpath '(field)) method)))))))))

(for-each (lambda (form)
            (pp form)
            (print))
          (foldl append '()
                 (map
                  (lambda (cls)
                    (let ((class-id (spec-prop 'index cls))
                          (class-name (spec-prop 'name cls)))
                      (map (lambda (method) (make-parser class-id class-name method))
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

(pp `(define (parse-method-payload bits)
       (bitmatch
        bits
        (((class-id 16)
          (method-id 16)
          (arguments bitstring))
         ,(make-class-dispatch ((sxpath `(// class)) amqp-xml-spec))))))
