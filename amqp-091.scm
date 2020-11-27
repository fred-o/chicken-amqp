;; -*- geiser-scheme-implementation: 'chicken -*-

(module amqp-091 *
  (import scheme
          (chicken base)
          (chicken format)
          (chicken bitwise)
          srfi-1
          bitstring)

  (define amqp-091-header
	(bitstring->string (bitconstruct
						("AMQP" bitstring)
						(#d0 8)
						(0 8) (9 8) (1 8))))
  
  (define-record frame type channel class-id method-id body-size properties payload)

  (define-record-printer (frame frm out)
    (fprintf out "#<frame type:~S channel:~S class:~S method:~S>"
             (frame-type frm)
             (frame-channel frm)
             (frame-class-id frm)
             (frame-method-id frm)))

  (define (frame-match? frm type class-id method-id)
	(define (cmp t v)
	  (cond ((not t) #t)
			((list? t) (member v t))
			(else (= t v))))
    (not (not (and (cmp type (frame-type frm))
				   (cmp class-id (frame-class-id frm))
				   (cmp method-id (frame-method-id frm)))))) 
  
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
     (has-content-type 1)
     (has-content-encoding 1)
     (has-headers 1)
     (has-delivery-mode 1)
     (has-priority 1)
     (has-correlation-id 1)
     (has-reply-to 1)
     (has-expiration 1)
     (has-message-id 1)
     (has-timestamp 1)
     (has-type 1)
     (has-user-id 1)
     (has-app-id 1)
     (padding 3)
     (content-type-size (* 8 has-content-type)) (content-type (* 8 content-type-size) bitstring)
     (content-encoding-size (* 8 has-content-encoding)) (content-encoding (* 8 content-encoding-size) bitstring)
     (headers-size (* 32 has-headers)) (headers (* 8 headers-size) bitstring)
     (delivery-mode (* 8 has-delivery-mode))
     (priority (* 8 has-priority))
     (correlation-id-size (* 8 has-correlation-id)) (correlation-id (* 8 correlation-id-size) bitstring)
     (reply-to-size (* 8 has-reply-to)) (reply-to (* 8 reply-to-size) bitstring)
     (expiration-size (* 8 has-expiration)) (expiration (* 8 expiration-size) bitstring)
     (message-id-size (* 8 has-message-id)) (message-id (* 8 message-id-size) bitstring)
     (timestamp (* 64 has-timestamp))
     (type-size (* 8 has-type)) (type (* 8 type-size) bitstring)
     (user-id-size (* 8 has-user-id)) (user-id (* 8 user-id-size) bitstring)
     (app-id-size (* 8 has-app-id)) (app-id (* 8 app-id-size) bitstring))
    (list class-id 0 body-size
          (foldl (lambda (acc x)
                   (if (= 1 (car x))
                       (cons (cons (cadr x) (caddr x)) acc)
                       acc))
                 '()
                 (list (list has-app-id 'app-id (bitstring->string app-id))
                       (list has-user-id 'user-id (bitstring->string user-id))
                       (list has-type 'type (bitstring->string type))
                       (list has-timestamp 'timestamp timestamp)
                       (list has-message-id 'message-id (bitstring->string message-id))
                       (list has-expiration 'expiration (bitstring->string expiration))
                       (list has-reply-to 'reply-to (bitstring->string reply-to))
                       (list has-correlation-id 'correlation-id (bitstring->string correlation-id))
                       (list has-priority 'priority priority)
                       (list has-delivery-mode 'delivery-mode delivery-mode)
                       (list has-headers 'headers (parse-table headers))
                       (list has-content-encoding 'content-encoding (bitstring->string content-encoding))
                       (list has-content-type 'content-type (bitstring->string content-type))))))))

(define (encode-headers-payload class-id weight body-size properties)
  (let* [(nullsafe-length (lambda (s) (if s (string-length s) 0)))
         (nullsafe-bitstring (lambda (s) (string->bitstring (if s s ""))))
         (nullsafe-bit (lambda (s) (if s 1 0)))
         (content-type (alist-ref 'content-type properties))
         (content-encoding (alist-ref 'content-encoding properties))
         (headers (alist-ref 'headers properties))
         (headers-payload (if headers (encode-table headers) (string->bitstring "")))
         (delivery-mode (alist-ref 'delivery-mode properties))
         (priority (alist-ref 'priority properties))
         (correlation-id (alist-ref 'correlation-id properties))
         (reply-to (alist-ref 'reply-to properties))
         (expiration (alist-ref 'expiration properties))
         (message-id (alist-ref 'message-id properties))
         (timestamp (alist-ref 'timestamp properties))
         (type (alist-ref 'type properties))
         (user-id (alist-ref 'user-id properties))
         (app-id (alist-ref 'app-id properties))]
    (bitconstruct
     (class-id 16)
     (weight 16)
     (body-size 64)
     ((nullsafe-bit content-type) 1)
     ((nullsafe-bit content-encoding) 1)
     ((nullsafe-bit headers) 1)
     ((nullsafe-bit delivery-mode) 1)
     ((nullsafe-bit priority) 1)
     ((nullsafe-bit correlation-id) 1)
     ((nullsafe-bit reply-to) 1)
     ((nullsafe-bit expiration) 1)
     ((nullsafe-bit message-id) 1)
     ((nullsafe-bit timestamp) 1)
     ((nullsafe-bit type) 1)
     ((nullsafe-bit app-id) 1)
     ((nullsafe-bit user-id) 1)
     (0 3) ;; padding
     ((nullsafe-length content-type) (if content-type 8 0)) ((nullsafe-bitstring content-type) bitstring)
     ((nullsafe-length content-encoding) (if content-encoding 8 0)) ((nullsafe-bitstring content-encoding) bitstring)
     ((if headers (/ (bitstring-length headers-payload) 8) 0) (if headers 32 0)) (headers-payload bitstring)
     ((if delivery-mode delivery-mode 0) (if delivery-mode 8 0))
     ((if priority priority 0) (if priority 8 0))
     ((nullsafe-length correlation-id) (if correlation-id 8 0)) ((nullsafe-bitstring correlation-id) bitstring)
     ((nullsafe-length reply-to) (if reply-to 8 0)) ((nullsafe-bitstring reply-to) bitstring)
     ((nullsafe-length expiration) (if expiration 8 0)) ((nullsafe-bitstring expiration) bitstring)
     ((nullsafe-length message-id) (if message-id 8 0)) ((nullsafe-bitstring message-id) bitstring)
     ((if timestamp timestamp 0) (if timestamp 64 0))
     ((nullsafe-length type) (if type 8 0)) ((nullsafe-bitstring type) bitstring)
     ((nullsafe-length user-id) (if user-id 8 0)) ((nullsafe-bitstring user-id) bitstring)
     ((nullsafe-length app-id) (if app-id 8 0)) ((nullsafe-bitstring app-id) bitstring))))

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

  (include "amqp-091-generated.scm"))
