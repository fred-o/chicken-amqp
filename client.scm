(import (chicken tcp)
        (chicken io)
        (chicken irregex)
        (chicken format)
        bitstring)

(include "parser.scm")

(define *amqp-header*
  (bitstring->string (bitconstruct
                      ("AMQP" bitstring)
                      (#d0 8)
                      (0 8) (9 8) (1 8))))

;; (define (make-table alist)
;;   (foldl (lambda (entry)))
;;   `(bitconstruct
;;     ,(map (lambda (entry)
;;             (((length (car entry)) 8) )))))

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
       (((Size 8)
         (Name (* Size 8) bitstring)
         (Rest bitstring))
        (let* ((result (parse-type Rest))
               (value (car result))
               (Rest (cdr result)))
          (append (list (list (bitstring->string Name) value)) (parse-table Rest)))))))

(define (make-connection-start-ok client-properties mechanism response locale)
  (bitconstruct
   (10 16)
   (11 16)
   (0 32)
   ((string-length mechanism) 8) (mechanism bitstring)
   ((string-length response) 32) (response bitstring)
   ((string-length locale) 8) (locale bitstring)))

(define (parse-frame str)
  (bitmatch
   str
   ((("AMQP" 32 bitstring)
     (Version bitstring))
    (error "Server rejected protocol version"))
   (((Type 8)
     (Channel 16)
     (Size 32)
     (Payload (* 8 Size) bitstring)
     (#xce)
     (Rest bitstring))
    (print "type " Type " channel " Channel)
    (cond
     ((= Type 1)
      (print "call parse-method")
      (let ((parsed-payload (parse-method Payload)))
        (cons (make-message Type Channel (car parsed-payload) (cadr parsed-payload) (caddr parsed-payload)) Rest)))
     (else (error "Unimplemented type " Type))))
   (else
    (print 'nope)
    (cons #f str))))

(define (make-frame type channel payload)
  (bitconstruct
   (type 8)
   (channel 16)
   ((/ (bitstring-length payload) 8) 32)
   (payload bitstring)
   (#xce 8)))

(define-record message type channel class method arguments)

(define-record-printer (message msg out)
  (fprintf out "#,(message type:~S channel:~S class:~S method:~S)" (message-type msg) (message-channel msg) (message-class msg) (message-method msg)))

;; (print (car (parse-frame (make-frame 1 0 (make-connection-start-ok '() "PLAIN" "" "en_US")))))

(define (amqp-client host port)
  (define-values (i o) (tcp-connect host port))
  (write-string *amqp-header* #f o)
  (letrec ((buf (->bitstring ""))
           (loop (lambda ()
                   (print "reading...")
                   (let ((got (read-string 1 i)))
                     (if (eq? #!eof got)
                         (print "Eof")
                         (let ((chunk (string-append got (read-buffered i))))
                           (print "read " (string-length chunk))
                           (bitstring-append! buf (string->bitstring chunk))
                           (let ((res (parse-frame buf)))
                             (print (car res))
                             (print (message-arguments (car res)))
                             (set! buf (cdr res))

                             (write-line (bitstring->string (make-frame 1 0 (make-connection-start-ok '() "PLAIN" "\x00local\x00panda4ever" "en_US"))) o)

                             ;; (print (car res))
                             (loop))))))))
    (loop)))

(amqp-client "localhost" 5672)

