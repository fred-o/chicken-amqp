(import (chicken tcp)
        (chicken io)
        (chicken irregex)
        bitstring)

(define *amqp-header*
  (bitstring->string (bitconstruct
                      ("AMQP" bitstring)
                      (#d0 8)
                      (0 8) (9 8) (1 8))))

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

(define (parse-connection-start-arguments bits)
  (bitmatch
   bits
   (((VersionMajor 8)
     (VersionMinor 8)
     (ServerPropertiesSize 32)
     (ServerProperties (* ServerPropertiesSize 8) bitstring)
     (MechanismsSize 32)
     (Mechanisms (* MechanismsSize 8) bitstring)
     (LocalesSize 32)
     (Locales (* LocalesSize 8) bitstring))
    (print "VersionMajor " VersionMajor " VersionMinor " VersionMinor)
    (print "ServerProperties" (parse-table ServerProperties))
    (print "Mechanisms " (bitstring->string Mechanisms))
    (print "Locales " (bitstring->string Locales)))))

(define (parse-method bits)
  (bitmatch
   bits
   (((ClassId 16)
     (MethodId 16)
     (Arguments bitstring))
    (print "ClassId " ClassId " MethodId " MethodId)
    (parse-connection-start-arguments Arguments))))

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
    (cond
     ((= Type 1)
      (cons (parse-method Payload) Rest))
     (else (error "Unimplemented type " Type))))
   (else (cons #f str))))

(define (amqp-client host port)
  (define-values (i o) (tcp-connect host port))
  (write-line *amqp-header* o)
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
                             (set! buf (cdr res))
                             ;; (print (car res))
                             (loop))))))))
    (loop)))

(amqp-client "localhost" 5672)

