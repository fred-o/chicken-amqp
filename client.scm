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

;; Data structures

(define-record connection in out input-thread parameters)

(define-record-printer (connection c out)
  (fprintf out "#<connection reader:~S ~S>"
           (connection-input-thread c)
           (connection-parameters c)))

(define-record channel id mailbox)

(define-record message type channel class method arguments)

(define-record-printer (message msg out)
  (fprintf out "#<message type:~S channel:~S class:~S method:~S>"
           (message-type msg)
           (message-channel msg)
           (message-class msg)
           (message-method msg)))

;; Fns

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
      (let ((parsed-payload (parse-method payload)))
        (cons (make-message type channel (car parsed-payload) (cadr parsed-payload) (caddr parsed-payload))
                rest)))
     ((= type 8)
      ;; This is a heartbeat
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

;; Send an AMQP message over the wire, thread safe
(define (amqp-send connection type channel payload)
  (write-string
   (bitstring->string (make-frame type (channel-id channel) payload))
   #f
   (connection-out connection)))

;; Receive the next message on the channel, blocking
(define (amqp-receive channel)
  (mailbox-receive! (channel-mailbox channel)))

(define (amqp-start-hearbeat connection channel)
  (letrec ((interval (/ (alist-ref 'heartbeat (connection-parameters connection)) 2))
           (payload (string->bitstring ""))
           (loop (lambda ()
                   (amqp-send connection 8 channel payload)
                   (sleep interval)
                   (loop))))
    (print "heartbeat interval " interval)
    (thread-start! loop)))

(define (amqp-manager connection channel)
  (letrec ((loop (lambda ()
                   (let ((msg (amqp-receive channel)))
                     (print msg)
                     (cond
                      ((and (equal? "connection" (message-class msg))
                            (equal? "close" (message-method msg)))
                       (amqp-send connection 1 channel (amqp:make-connection-close-ok))
                       (print "server closed connection: "
                              (alist-ref 'reply-code (message-arguments msg))
                              " - "
                              (alist-ref 'reply-text (message-arguments msg)))
                       ;; release resources
                       (thread-terminate! (connection-input-thread connection))
                       (close-input-port (connection-in connection))
                       (close-output-port (connection-out connection))))
                     (loop)))))
    (print "starting manager thread")
    (thread-start! loop)))

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
                                (let* ((message/rest (parse-frame buf)))
                                  (mailbox-send! (channel-mailbox default-channel) (car message/rest))
                                  (set! buf (cdr message/rest))
                                  (read-loop))))))))
    (let* ((input-thread (make-thread read-loop))
           (connection (make-connection i o input-thread #f)))
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
        (print msg)
        (connection-parameters-set! connection args)
        (amqp-send connection 1 default-channel
                   (amqp:make-connection-tune-ok (alist-ref 'channel-max args)
                                                 (alist-ref 'frame-max args)
                                                 (alist-ref 'heartbeat args))))
      ;; open connection
      (amqp-send connection 1 default-channel
                 (amqp:make-connection-open "/" "" 0))
      (let* ((msg (amqp-receive default-channel))
             (args (message-arguments msg)))
        (print msg args))
      ;; start sending heartbeats
      (amqp-start-hearbeat connection default-channel)
      ;; start manager thread
      (amqp-manager connection default-channel)
      connection)))


