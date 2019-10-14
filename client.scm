(import (chicken tcp)
        (chicken io)
        (chicken irregex)
        (chicken format)
        srfi-18
        mailbox
        bitstring)

(include "parser.scm")

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
    (cond
     ((= type 1)
      (print "call parse-method")
      (let ((parsed-payload (parse-method payload)))
        (cons (make-message type channel (car parsed-payload) (cadr parsed-payload) (caddr parsed-payload))
                rest)))
     ((= type 8)
      (print "received heartbeat" payload)
      (cons #f rest))
     (else (error "Unimplemented type " type))))
   (else
    (error "blargh"))))

(define (make-frame type channel payload)
  (bitconstruct
   (type 8)
   (channel 16)
   ((/ (bitstring-length payload) 8) 32)
   (payload bitstring)
   (#xce 8)))

;; Data structures

(define-record connection in out input-thread)

(define-record channel id mailbox)

(define-record message type channel class method arguments)

(define-record-printer (message msg out)
  (fprintf out "#,(message type:~S channel:~S class:~S method:~S)" (message-type msg) (message-channel msg) (message-class msg) (message-method msg)))

;; Send an AMQP message over the wire, thread safe
(define (amqp-send connection type channel payload)
  (print payload)
  (write-string
   (bitstring->string (make-frame type (channel-id channel) payload))
   #f
   (connection-out connection)))

;; Receive the next message on the channel, blocking
(define (amqp-receive channel)
  (mailbox-receive! (channel-mailbox channel)))

;; Connect to AMQP server, perform handshake
(define (amqp-connect host port)
  (define-values (i o) (tcp-connect host port))
  (letrec ((buf (->bitstring ""))
           (default-channel (make-channel 0 (make-mailbox)))
           (read-loop (lambda ()
                        (let ((first-byte (read-string 1 i)))
                          (if (eq? #!eof first-byte)
                              (print "Eof")
                              (begin
                                (bitstring-append! buf (string->bitstring (string-append first-byte (read-buffered i))))
                                (let ((message-and-rest (parse-frame buf)))
                                  (mailbox-send! (channel-mailbox default-channel) (car message-and-rest))
                                  ;; (print (car message-and-rest))
                                  (set! buf (cdr message-and-rest))
                                  (read-loop))))))))
    (let* ((input-thread (make-thread read-loop))
           (connection (make-connection i o input-thread)))
      (thread-start! input-thread)
      ;; handshake
      (write-string *amqp-header* #f o)
      ;; security
      (let ((msg (amqp-receive default-channel)))
        (print msg)
        (amqp-send connection 1 default-channel
                   (amqp:make-connection-start-ok  '() "PLAIN" "\x00local\x00panda4ever" "en_US")))
      ;; tune
      (let* ((msg (amqp-receive default-channel))
             (args (message-arguments msg)))
        (print msg args)
        (amqp-send connection 1 default-channel
                   (amqp:make-connection-tune-ok (car (alist-ref 'channel-max args))
                                                 (car (alist-ref 'frame-max args))
                                                 (car (alist-ref 'heartbeat args)))))
      ;; open connection
      (amqp-send connection 1 default-channel
                 (amqp:make-connection-open "/" "" 0))
      (let* ((msg (amqp-receive default-channel))
             (args (message-arguments msg)))
        (print msg args))
      connection)))

(thread-join! (connection-input-thread (amqp-connect "localhost" 5672)))


