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

(define-record connection in out parameters threads dispatchers)

(define-record-printer (connection c out)
  (fprintf out "#<connection reader:~S>"
           (connection-parameters c)))

(define-record channel id mailbox)

(define-record message type channel class-id class method-id method arguments)

(define-record-printer (message msg out)
  (fprintf out "#<message type:~S channel:~S class:~S method:~S>"
           (message-type msg)
           (message-channel msg)
           (message-class msg)
           (message-method msg)))

;; Fns

(define (encode-table props)
   (apply bitstring-append
          (map (lambda (prop)
                 (let ((key (car prop))
                       (val (cdr prop)))
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
        (cons (apply make-message (append (list type channel) parsed-payload))
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
  (let ((channel-id (if (channel? channel) (channel-id channel) channel)))
    (write-string
     (bitstring->string (make-frame type channel payload))
     #f
     (connection-out connection))))

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

;; (define (amqp-manager connection channel)
;;   (letrec ((loop (lambda ()
;;                    (let ((msg (amqp-receive channel)))
;;                      (print msg)
;;                      (cond
;;                       ((and (equal? "connection" (message-class msg))
;;                             (equal? "close" (message-method msg)))
;;                        (amqp-send connection 1 channel (amqp:make-connection-close-ok))
;;                        (print "server closed connection: "
;;                               (alist-ref 'reply-code (message-arguments msg))
;;                               " - "
;;                               (alist-ref 'reply-text (message-arguments msg)))
;;                        ;; release resources
;;                        (thread-terminate! (connection-input-thread connection))
;;                        (close-input-port (connection-in connection))
;;                        (close-output-port (connection-out connection))))
;;                      (loop)))))
;;     (print "starting manager thread")
;;     (thread-start! loop)))

(define (dispatch-register! connection pattern)
  (let ((mb (make-mailbox)))
    (connection-dispatchers-set! connection (cons (cons pattern mb)
                                                  (connection-dispatchers connection)))
    mb))

;; (define (dispatch-unregister! connection pattern))

(define (pattern-match? pattern msg)
  (call/cc (lambda (return)
             (for-each (lambda (part)
                         (let ((field (car part))
                               (value (cdr part)))
                         (cond
                          ((and (eq? 'class field) (not (equal? value (message-class msg)))) (return #f))
                          ((and (eq? 'class-id field) (not (equal? value (message-class-id msg))) (return #f))))))
                       pattern)
             (return #t))))

(define (dispatcher connection)
  (lambda ()
    (letrec ((in (connection-in connection))
             (buf (->bitstring ""))
             (loop (lambda ()
                     (let ((first-byte (read-string 1 in)))
                       (if (eq? #!eof first-byte)
                           (print "Eof")
                           (begin
                             (bitstring-append! buf (string->bitstring (string-append first-byte (read-buffered in))))
                             (let* ((message/rest (parse-frame buf))
                                    (msg (car message/rest)))
                               (for-each (lambda (disp)
                                           (if (pattern-match? (car disp) msg) (mailbox-send! (cdr disp) msg)))
                                         (connection-dispatchers connection))
                               (set! buf (cdr message/rest))
                               (loop))))))))
      (loop))))

(define (manager connection)
  (lambda ()
    (letrec ((mb (dispatch-register! connection '((class-id . 10))))
             (loop (lambda ()
                     (let ((msg (mailbox-receive! mb)))
                       (print 'msg msg)
                       (loop)))))
      (write-string *amqp-header* #f (connection-out connection))
      (loop))))

(define (amqp-connect host port)
  (define-values (i o) (tcp-connect host port))
  (let* ((connection (make-connection i o #f '() '())))
    (connection-threads-set! connection
                             (list
                              (thread-start! (dispatcher connection))
                              (thread-start! (manager connection))))
    connection))

;; Connect to AMQP server, perform handshake
;; (define (amqp-connect host port)
;;   (define-values (i o) (tcp-connect host port))
;;   (letrec ((buf (->bitstring ""))
;;            (default-channel (make-channel 0 (make-mailbox)))
;;            (read-loop (lambda ()
;;                         (let ((first-byte (read-string 1 i)))
;;                           (if (eq? #!eof first-byte)
;;                               (print "Eof")
;;                               (begin
;;                                 (bitstring-append! buf (string->bitstring (string-append first-byte (read-buffered i))))
;;                                 (let* ((message/rest (parse-frame buf))
;;                                        (msg (car message/rest)))
;;                                   (if msg (mailbox-send! (channel-mailbox default-channel) msg))
;;                                   (set! buf (cdr message/rest))
;;                                   (read-loop))))))))
;;     (let* ((input-thread (make-thread read-loop))
;;            (connection (make-connection i o input-thread #f)))
;;       (thread-start! input-thread)
;;       ;; handshake
;;       (write-string *amqp-header* #f o)
;;       ;; security
;;       (let ((msg (amqp-receive default-channel)))
;;         (print msg)
;;         (amqp-send connection 1 default-channel
;;                    (amqp:make-connection-start-ok  '(("connection_name" . "my awesome client")) "PLAIN" "\x00local\x00panda4ever" "en_US")))
;;       ;; tune
;;       (let* ((msg (amqp-receive default-channel))
;;              (args (message-arguments msg)))
;;         (print msg)
;;         (connection-parameters-set! connection args)
;;         (amqp-send connection 1 default-channel
;;                    (amqp:make-connection-tune-ok (alist-ref 'channel-max args)
;;                                                  (alist-ref 'frame-max args)
;;                                                  (alist-ref 'heartbeat args))))
;;       ;; open connection
;;       (amqp-send connection 1 default-channel
;;                  (amqp:make-connection-open "/" "" 0))
;;       (let* ((msg (amqp-receive default-channel))
;;              (args (message-arguments msg)))
;;         (print msg args))
;;       ;; start sending heartbeats
;;       (amqp-start-hearbeat connection default-channel)
;;       ;; start manager thread
;;       (amqp-manager connection default-channel)
;;       connection)))


