(module amqp (amqp-connect
              connection-threads)

(import scheme (chicken base) (chicken syntax)
        (chicken tcp)
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
  (fprintf out "#<connection parameters: ~S>"
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
                               (when msg
                                 (for-each (lambda (disp)
                                             (if (pattern-match? (car disp) msg) (mailbox-send! (cdr disp) msg)))
                                           (connection-dispatchers connection))
                                 (set! buf (cdr message/rest)))
                               (loop))))))))
      (loop))))

(define (manager connection)
  (lambda ()
    (letrec ((mb (dispatch-register! connection '((class-id . 10))))
             (loop (lambda ()
                     (let* ((msg (mailbox-receive! mb))
                            (method (message-method msg)))
                       (print 'msg msg)
                       (cond
                        ((equal? "start" method)
                         (amqp-send connection 1 0
                                    (amqp:make-connection-start-ok  '(("connection_name" . "my awesome client"))
                                                                    "PLAIN"
                                                                    "\x00local\x00panda4ever"
                                                                    "en_US")))
                        ((equal? "tune" method)
                         (let* ((args (message-arguments msg)))
                           (connection-parameters-set! connection args)
                           (amqp-send connection 1 0
                                      (amqp:make-connection-tune-ok (alist-ref 'channel-max args)
                                                                    (alist-ref 'frame-max args)
                                                                    (alist-ref 'heartbeat args)))
                           (amqp-send connection 1 0
                                      (amqp:make-connection-open "/" "" 0))))
                        ((equal? "open-ok" method)
                         ;; start heartbeats)
                         (connection-threads-set! connection
                                                  (append (connection-threads connection)
                                                          (list (thread-start! (heartbeats connection))))))
                        ))
                     (loop))))
      ;; start 
      (write-string *amqp-header* #f (connection-out connection))
      (loop))))

(define (heartbeats connection)
  (letrec ((interval (/ (alist-ref 'heartbeat (connection-parameters connection)) 2))
           (payload (string->bitstring ""))
           (loop (lambda ()
                   (amqp-send connection 8 0 payload)
                   (sleep interval)
                   (loop))))
    (loop)))

(define (amqp-connect host port)
  (define-values (i o) (tcp-connect host port))
  (let* ((connection (make-connection i o #f '() '())))
    (connection-threads-set! connection
                             (list
                              (thread-start! (dispatcher connection))
                              (thread-start! (manager connection))))
    connection))

)
