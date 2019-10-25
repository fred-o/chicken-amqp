(include "amqp091.scm")

(module amqp-core *

(import scheme (chicken base) (chicken syntax)
        (chicken tcp)
        (chicken io)
        (chicken irregex)
        (chicken format)
        (chicken bitwise)
        srfi-1
        srfi-18
        srfi-88
        mailbox
        bitstring
        amqp091)

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

(define-record channel id method-mailbox content-mailbox connection)

(define-record-printer (channel ch out)
  (fprintf out "#<channel: ~S>" (channel-id channel)))

;; (define-record message type channel class-id class method-id method arguments)

;; Fns

(define (next-channel-id connection)
  (let ((lock (connection-lock connection))
        (id (connection-channel-id-seq connection)))
    (mutex-lock! lock)
    (connection-channel-id-seq-set! connection (+ 1 id))
    (mutex-unlock! lock)
    id))

;; Send an AMQP message over the wire, thread safe
(define (amqp-send channel type payload)
  (write-string
   (bitstring->string (encode-frame type (channel-id channel) payload))
   #f
   (connection-out (channel-connection channel))))

;; Receive the next message on the channel, blocking
(define (receive-frame channel #!key (mailbox 'method))
  (mailbox-receive! (cond ((eq? 'method mailbox)
                           (channel-method-mailbox channel))
                          ((eq? 'content mailbox)
                           (channel-content-mailbox channel)))))

(define (expect-frame channel class-id method-id #!key (mailbox 'method))
  (let ((frm (receive-frame channel mailbox: mailbox)))
    (if (and (equal? class-id (frame-class-id frm))
             (equal? method-id (frame-method-id frm)))
        frm
        (raise (sprintf "(channel ~S): expected class ~S/method ~S, got ~S/~S"
                        (channel-id channel)
                        class-id method-id
                        (frame-class-id frm) (frame-method-id frm))))))

(define (dispatch-register! connection pattern)
  (let [(lock (connection-lock connection))
        (mb (make-mailbox))]
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

(define (dispatch-pattern-match? pattern frm)
  (pattern (frame-type frm)
           (frame-channel frm)
           (frame-class-id frm)
           (frame-method-id frm)))

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
                             (letrec [(parse-loop (lambda ()
                                                    (let* ((message/rest (parse-frame buf))
                                                           (frm (car message/rest)))
                                                      (set! buf (cdr message/rest))
                                                      (when frm
                                                        (print "dispatching: " frm)
                                                        (for-each (lambda (disp)
                                                                    (when (dispatch-pattern-match? (car disp) frm)
                                                                      (mailbox-send! (cdr disp) frm)))
                                                                  (connection-dispatchers connection))
                                                        (parse-loop)))))]
                               (parse-loop))))
                       (loop)))))
      (loop))))

(define (handshake channel)
  (lambda ()
    (let* [(conn (channel-connection channel))
           (mb (dispatch-register! conn (lambda (type channel class-id method-id) (= class-id 10))))
           (done #f)]
      (letrec ((loop (lambda ()
                       (let* ((frm (mailbox-receive! mb))
                              (method-id (frame-method-id frm)))
                         (cond
                          ((equal? 10 method-id)
                           (amqp-send channel 1
                                      (make-connection-start-ok  '(("connection_name" . "my awesome client"))
                                                                 "PLAIN"
                                                                 "\x00local\x00panda4ever"
                                                                 "en_US")))
                          ((equal? 30 method-id)
                           (let* ((args (frame-properties frm)))
                             (connection-parameters-set! conn args)
                             (amqp-send channel 1
                                        (make-connection-tune-ok (alist-ref 'channel-max args)
                                                                 (alist-ref 'frame-max args)
                                                                 (alist-ref 'heartbeat args)))
                             (amqp-send channel 1
                                        (make-connection-open "/"))))
                          ((equal? 41 method-id)
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
         (default-channel (make-channel 0 (make-mailbox) (make-mailbox) connection))
         (dispatcher-thread (thread-start! (dispatcher connection)))
         (handshake-thread (thread-start! (make-thread (handshake default-channel)))))
    ;; wait for handshake to finish
    (thread-join! handshake-thread)
    ;; start the heartbeat thread
    (connection-threads-set! connection (list dispatcher-thread
                                              (thread-start! (heartbeats default-channel))))
    ;; ...annnd we are done!
    connection)))

(module amqp-primitives *

(import scheme (chicken base) (chicken syntax)
        mailbox
        bitstring
        amqp-core
        amqp091)

;; AMQP primitives API

(define (receive-message channel)
  (let* [(mthd (expect-frame channel 60 60 mailbox: 'content))
         (hdrs (receive-frame channel mailbox: 'content))
         (buf (string->bitstring ""))]
    (print '---> mthd (frame-properties mthd))
    (print '---> hdrs (frame-properties hdrs))))

(define (channel-open conn)
  (let* [(id (next-channel-id conn))
         (ch (make-channel id
                           ;; method messages go to regular mailbox
                           (dispatch-register! conn
                                               (lambda (type channel-id class-id method-id)
                                                 (and (= channel-id id)
                                                      (= type 1)
                                                      (not (and (= class-id 60)
                                                                (= method-id 60))))))
                           ;; content-bearing messages go to the content mailbox
                           (dispatch-register! conn
                                               (lambda (type channel-id class-id method-id)
                                                 (and (= channel-id id)
                                                      (or (= type 2)
                                                          (= type 3)
                                                          (and (= class-id 60)
                                                               (= method-id 60))))))
                           conn))]
    (amqp-send ch 1 (make-channel-open))
    (expect-frame ch 20 11)
    ch))

(define (queue-declare channel queue #!key (passive 0) (durable 0) (exclusive 0) (auto-delete 0) (no-wait 0))
  (amqp-send channel 1 (make-queue-declare queue passive durable exclusive auto-delete no-wait '()))
  (let [(reply (expect-frame channel 50 11))]
    (alist-ref 'queue (frame-properties reply))))

(define (queue-bind channel queue exchange routing-key #!key (no-wait 0))
  (amqp-send channel 1 (make-queue-bind queue exchange routing-key no-wait '()))
  (expect-frame channel 50 21))

(define (basic-consume channel queue  #!key (no-local 0) (no-ack 0) (exclusive 0) (no-wait 0))
  (let [(tag "2abc")]
    (amqp-send channel 1 (make-basic-consume queue tag no-local no-ack exclusive no-wait '()))
    (expect-frame channel 60 21)))

)
