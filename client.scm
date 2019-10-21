(import scheme (chicken base) (chicken syntax)
        (chicken tcp)
        (chicken io)
        (chicken irregex)
        (chicken format)
        srfi-1
        srfi-18
        srfi-88
        mailbox
        bitstring)

(include "parser.scm")

(define *amqp-header*
  (bitstring->string (bitconstruct
                      ("AMQP" bitstring)
                      (#d0 8)
                      (0 8) (9 8) (1 8))))

;; Data structures

(define-record connection in out parameters threads dispatchers lock channel-id-seq)

(define-record-printer (connection c out)
  (fprintf out "#<connection parameters: ~S>"
           (connection-parameters c)))

(define-record channel id mailbox connection)

(define-record message type channel class-id class method-id method arguments)

(define-record-printer (message msg out)
  (fprintf out "#<message type:~S channel:~S class:~S method:~S>"
           (message-type msg)
           (message-channel msg)
           (message-class msg)
           (message-method msg)))

;; Fns

(define (next-channel-id connection)
  (let ((lock (connection-lock connection))
        (id (connection-channel-id-seq connection)))
    (mutex-lock! lock)
    (connection-channel-id-seq-set! connection (+ 1 id))
    (mutex-unlock! lock)
    id))

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
(define (amqp-send channel type payload)
  (write-string
   (bitstring->string (make-frame type (channel-id channel) payload))
   #f
   (connection-out (channel-connection channel))))

;; Receive the next message on the channel, blocking
(define (amqp-receive channel)
  (mailbox-receive! (channel-mailbox channel)))

(define (amqp-expect channel class method)
  (let ((msg (amqp-receive channel)))
    (if (and (equal? class (message-class msg))
             (equal? method (message-method msg)))
        msg
        (raise (sprintf "(channel ~S): expected class ~S/method ~S, got ~S/~S"
                        (channel-id channel)
                        class method
                        (message-class msg) (message-method msg))))))

(define (dispatch-register! connection pattern)
  (let ((mb (make-mailbox))
        (lock (connection-lock connection)))
    (mutex-lock! lock)
    (connection-dispatchers-set! connection (cons (cons pattern mb)
                                                  (connection-dispatchers connection)))
    (mutex-unlock! lock)
    mb))

(define (dispatch-unregister! connection mailbox)
  (let ((lock (connection-lock connection)))
    (mutex-lock! lock)
    (connection-dispatchers-set! connection 
                                 (filter (lambda (dispatch) (not (eq? mailbox (cdr dispatch))))
                                         (connection-dispatchers connection)))
    (mutex-unlock! lock)))

(define (dispatch-pattern-match? pattern msg)
  (call/cc (lambda (return)
             (for-each (lambda (part)
                         (let ((field (car part))
                               (value (cdr part)))
                           (cond
                            ((and (eq? 'type field) (not (equal? value (message-type msg)))) (return #f))
                            ((and (eq? 'class field) (not (equal? value (message-class msg)))) (return #f))
                            ((and (eq? 'class-id field) (not (equal? value (message-class-id msg))) (return #f)))
                            ((and (eq? 'channel field) (not (equal? value (message-channel msg))))) (return #f))))
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
                                             (if (dispatch-pattern-match? (car disp) msg) (mailbox-send! (cdr disp) msg)))
                                           (connection-dispatchers connection))
                                 (set! buf (cdr message/rest)))
                               (loop))))))))
      (loop))))

(define (handshake channel)
  (lambda ()
    (let* ((conn (channel-connection channel))
           (mb (dispatch-register! conn '((class-id . 10))))
           (done #f))
      (letrec ((loop (lambda ()
                       (let* ((msg (mailbox-receive! mb))
                              (method (message-method msg)))
                         (print 'msg msg)
                         (cond
                          ((equal? "start" method)
                           (amqp-send channel 1
                                      (amqp:make-connection-start-ok  '(("connection_name" . "my awesome client"))
                                                                      "PLAIN"
                                                                      "\x00local\x00panda4ever"
                                                                      "en_US")))
                          ((equal? "tune" method)
                           (let* ((args (message-arguments msg)))
                             (connection-parameters-set! conn args)
                             (amqp-send channel 1
                                        (amqp:make-connection-tune-ok (alist-ref 'channel-max args)
                                                                      (alist-ref 'frame-max args)
                                                                      (alist-ref 'heartbeat args)))
                             (amqp-send channel 1
                                        (amqp:make-connection-open "/"))))
                          ((equal? "open-ok" method)
                           (set! done #t)))
                         (if done
                             (dispatch-unregister! conn mb) ;; clean up 
                             (loop))))))
        ;; start 
        (write-string *amqp-header* #f (connection-out conn))
        (loop)))))

(define (heartbeats channel)
  (lambda ()
    (letrec ((interval (/ (alist-ref 'heartbeat (connection-parameters (channel-connection channel))) 2))
             (payload (string->bitstring ""))
             (loop (lambda ()
                     (amqp-send channel 8 payload)
                     (sleep interval)
                     (loop))))
      (loop))))

(define (amqp-connect host port)
  (define-values (i o) (tcp-connect host port))
  (let* ((connection (make-connection i o #f '() '() (make-mutex) 1))
         (default-channel (make-channel 0 (make-mailbox) connection))
         (dispatcher-thread (thread-start! (dispatcher connection)))
         (handshake-thread (thread-start! (make-thread (handshake default-channel)))))
    ;; wait for handshake to finish
    (thread-join! handshake-thread)
    ;; start the heartbeat thread
    (connection-threads-set! connection (list dispatcher-thread
                                              (thread-start! (heartbeats default-channel))))
    ;; ...annnd we are done!
    connection))

;; client api

(define (channel-open conn)
  (let* ((id (next-channel-id conn))
         (mb (dispatch-register! conn '((channel . id) (type . 1))))
         (ch (make-channel id mb conn)))
    (amqp-send ch 1 (amqp:make-channel-open))
    (amqp-expect ch "channel" "open-ok")
    ch))

(define (default-0) 0)

(define (queue-declare channel name . args)
  (amqp-send channel 1 (amqp:make-queue-declare name
                                                (get-keyword passive: args default-0)
                                                (get-keyword durable: args default-0)
                                                (get-keyword exclusive: args default-0)
                                                (get-keyword auto-delete: args default-0)
                                                (get-keyword no-wait: args default-0)
                                                '()))
  (amqp-expect channel "queue" "declare-ok"))
;  (amqp-receive channel))

;; (define (exchange-declare-passive name))

;; (define (queue-declare channel queue-name durable auto-delete)
;;   (amqp-send (channel-connection channel)))
