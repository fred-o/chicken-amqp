(module amqp091 *
  (import scheme
          (chicken base)
          (chicken syntax)
          (chicken format)
          (chicken bitwise)
          srfi-1
          bitstring)

  (define-record frame type channel class-id method-id body-size properties payload)

  (define-record-printer (frame frm out)
    (fprintf out "#<frame type:~S channel:~S class:~S method:~S>"
             (frame-type frm)
             (frame-channel frm)
             (frame-class-id frm)
             (frame-method-id frm)))

  (define (encode-table props)
    (apply bitstring-append
           (map (lambda (prop)
                  (let [(key (car prop))
                        (val (cdr prop))]
                    (cond ((string? val) (bitconstruct ((string-length key) 8) (key bitstring) (#\S) ((string-length val) 32) (val bitstring))))))
                props)))

  (define (parse-type bits)
    (bitmatch
     bits
     (((#\F) (Size 32) (Table (* 8 Size) bitstring) (Rest bitstring)) (cons (parse-table Table) Rest))
     (((#\S) (Size 32) (String (* 8 Size) bitstring) (Rest bitstring)) (cons (bitstring->string String) Rest))
     (((#\t) (Bool boolean) (Rest bitstring)) (cons Bool Rest))
     (((Type 8) (Rest bitstring)) (error "Unknown table type " Type))))

  (define (parse-table bits)
    (if (= 0 (bitstring-length bits))
        '()
        (bitmatch
         bits
         (((size 8)
           (name (* size 8) bitstring)
           (rest bitstring))
          (let* ((result (parse-type rest))
                 (value (car result))
                 (rest (cdr result)))
            (cons (cons (bitstring->string name) value) (parse-table rest)))))))

(define (bit-value n pos) (if (bit->boolean n pos) 1 0))

(define (parse-headers-payload bits)
  (bitmatch
   bits
   (((class-id 16)
     (weight 16)
     (body-size 64)
     (flags 16)
     (content-type-size (* 8 (bit-value flags 15))) (content-type (* 8 content-type-size) bitstring)
     (content-encoding-size (* 8 (bit-value flags 14))) (content-encoding (* 8 content-encoding-size) bitstring)
     (headers-size (* 32 (bit-value flags 13))) (headers (* 8 headers-size) bitstring)
     (delivery-mode (* 8 (bit-value flags 12)))
     (priority (* 8 (bit-value flags 11)))
     (correlation-id-size (* 8 (bit-value flags 10))) (correlation-id (* 8 correlation-id-size) bitstring)
     (reply-to-size (* 8 (bit-value flags 9))) (reply-to (* 8 reply-to-size) bitstring)
     (expiration-size (* 8 (bit-value flags 8))) (expiration (* 8 expiration-size) bitstring)
     (message-id-size (* 8 (bit-value flags 7))) (message-id (* 8 message-id-size) bitstring)
     (timestamp (* 64 (bit-value flags 6)))
     (type-size (* 8 (bit-value flags 5))) (type (* 8 type-size) bitstring)
     (user-id-size (* 8 (bit-value flags 4))) (user-id (* 8 user-id-size) bitstring)
     (app-id-size (* 8 (bit-value flags 3))) (app-id (* 8 app-id-size) bitstring))
    (list class-id 0 body-size
          (list (cons 'content-type (bitstring->string content-type))
                (cons 'content-encoding (bitstring->string content-encoding))
                (cons 'headers (parse-table headers))
                (cons 'delivery-mode delivery-mode)
                (cons 'priority priority)
                (cons 'correlation-id (bitstring->string correlation-id))
                (cons 'reply-to (bitstring->string reply-to))
                (cons 'expiration (bitstring->string expiration))
                (cons 'message-id (bitstring->string message-id))
                (cons 'timestamp timestamp)
                (cons 'type (bitstring->string type))
                (cons 'user-id (bitstring->string user-id))
                (cons 'app-id (bitstring->string app-id)))))))

(define (encode-headers-payload class-id weight body-size properties)
  (let* [(nullsafe-length (lambda (s) (if s (string-length s) 0)))
         (nullsafe-bitstring (lambda (s) (string->bitstring (if s s ""))))
         (content-type (alist-ref 'content-type properties))
         (content-encoding (alist-ref 'content-encoding properties))
         (headers (alist-ref 'headers properties))
         (headers-payload (if headers (encode-table headers) (string->bitstring "")))
         (delivery-mode (alist-ref 'delivery-mode properties))
         (priority (alist-ref 'priority properties))
         (correlation-id (alist-ref 'correlation-id properties))
         (reply-to (alist-ref 'reply-to properties))
         (expiration (alist-ref 'expiration properties))
         (message-id (alist-ref 'message-id properties))]
    (bitconstruct
     (class-id 16)
     (weight 16)
     (body-size 64)
     ((bitwise-ior (if content-type (arithmetic-shift 1 15) 0)
                   (if content-encoding (arithmetic-shift 1 14) 0)
                   (if headers (arithmetic-shift 1 13) 0)
                   (if delivery-mode (arithmetic-shift 1 12) 0)
                   (if priority (arithmetic-shift 1 11) 0)
                   (if correlation-id (arithmetic-shift 1 10) 0)
                   (if reply-to (arithmetic-shift 1 9) 0)
                   (if expiration (arithmetic-shift 1 8) 0)
                   (if message-id (arithmetic-shift 1 7) 0)) 16)
     ((nullsafe-length content-type) (if content-type 8 0)) ((nullsafe-bitstring content-type) bitstring)
     ((nullsafe-length content-encoding) (if content-encoding 8 0)) ((nullsafe-bitstring content-encoding) bitstring)
     ((if headers (/ (bitstring-length headers-payload) 8) 0) (if headers 32 0)) (headers-payload bitstring)
     ((if delivery-mode delivery-mode 0) (if delivery-mode 8 0))
     ((if priority priority 0) (if priority 8 0))
     ((nullsafe-length correlation-id) (if correlation-id 8 0)) ((nullsafe-bitstring correlation-id) bitstring)
     ((nullsafe-length reply-to) (if reply-to 8 0)) ((nullsafe-bitstring reply-to) bitstring)
     ((nullsafe-length expiration) (if expiration 8 0)) ((nullsafe-bitstring expiration) bitstring)
     ((nullsafe-length message-id) (if message-id 8 0)) ((nullsafe-bitstring message-id) bitstring)

     )))

(define (parse-frame str)
  (bitmatch
   str
   ((("AMQP" 32 bitstring)
     (Version bitstring))
    (error "Server rejected protocol version"))
   (((type 8)
     (channel 16)
     (payload-size 32)
     (payload (* 8 payload-size) bitstring)
     (#xce)
     (rest bitstring))
    (case type
      ((1) (cons (apply make-frame (append (list type channel) (parse-method-payload payload) '(#f)))
                 rest))
      ((2) (cons (apply make-frame (append (list type channel) (parse-headers-payload payload) '(#f)))
                 rest))
      ((3) (cons (make-frame type channel 0 0 #f #f payload)
                 rest))
      ((8) (cons #f rest)) ;; This is a heartbeat
      (else (error "Unimplemented type " type))))
   (else
    ;; no match (yet)
    (cons #f str))))

(define (encode-frame type channel payload)
  (bitconstruct
   (type 8)
   (channel 16)
   ((/ (bitstring-length payload) 8) 32)
   (payload bitstring)
   (#xce 8)))

  (include "parser.scm"))
