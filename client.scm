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

(define-record-printer (channel ch out)
  (fprintf out "#<channel: ~S>" id))

(define-record frame type channel class-id method-id properties payload)

(define-record-printer (frame msg out)
  (fprintf out "#<frame type:~S channel:~S class:~S method:~S>"
           (frame-type msg)
           (frame-channel msg)
           (frame-class-id msg)
           (frame-method-id msg)))

;; (define-record method-frame class-id method-id arguments)

;; (define-record header type channel class-id)

;; (define-record content payload)

(define-record message type channel class-id class method-id method arguments)


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
    (case type
     ((1) (let [(parsed-payload (parse-method payload))]
            (cons (apply make-frame (append (list type channel) parsed-payload '(#f)))
                  rest)))
     ((2) (let [(parsed-payload (parse-headers-payload payload))]
            (cons (apply make-frame (append (list type channel) parsed-payload '(#f)))
                  rest)))
     ((3) ;; frame body
      (print "body: type " type " channel " channel " payload-size " payload-size)
      (cons #f rest))
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
    (list class-id 0
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

;; Send an AMQP message over the wire, thread safe
(define (amqp-send channel type payload)
  (write-string
   (bitstring->string (encode-frame type (channel-id channel) payload))
   #f
   (connection-out (channel-connection channel))))

;; Receive the next message on the channel, blocking
(define (amqp-receive channel)
  (mailbox-receive! (channel-mailbox channel)))

(define (amqp-expect channel class-id method-id)
  (let ((msg (amqp-receive channel)))
    (if (and (equal? class-id (frame-class-id msg))
             (equal? method-id (frame-method-id msg)))
        msg
        (raise (sprintf "(channel ~S): expected class ~S/method ~S, got ~S/~S"
                        (channel-id channel)
                        class-id method-id
                        (frame-class-id msg) (frame-method-id msg))))))

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
                            ((and (eq? 'type field) (not (equal? value (frame-type msg)))) (return #f))
                            ((and (eq? 'class field) (not (equal? value (frame-class msg)))) (return #f))
                            ((and (eq? 'class-id field) (not (equal? value (frame-class-id msg))) (return #f)))
                            ((and (eq? 'channel-id field) (not (equal? value (frame-channel msg)))) (return #f)))))
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
                             (letrec [(parse-loop (lambda ()
                                                    (let* ((message/rest (parse-frame buf))
                                                           (msg (car message/rest)))
                                                      (print msg)
                                                      (set! buf (cdr message/rest))
                                                      (when msg
                                                        (for-each (lambda (disp)
                                                                    (when (dispatch-pattern-match? (car disp) msg)
                                                                      (mailbox-send! (cdr disp) msg)))
                                                                  (connection-dispatchers connection))
                                                        (parse-loop)))))]
                               (parse-loop))))
                       (loop)))))
      (loop))))

(define (handshake channel)
  (lambda ()
    (let* ((conn (channel-connection channel))
           (mb (dispatch-register! conn '((class-id . 10))))
           (done #f))
      (letrec ((loop (lambda ()
                       (let* ((msg (mailbox-receive! mb))
                              (method-id (frame-method-id msg)))
                         (print 'msg msg)
                         (cond
                          ((equal? 10 method-id)
                           (amqp-send channel 1
                                      (amqp:make-connection-start-ok  '(("connection_name" . "my awesome client"))
                                                                      "PLAIN"
                                                                      "\x00local\x00panda4ever"
                                                                      "en_US")))
                          ((equal? 30 method-id)
                           (let* ((args (frame-properties msg)))
                             (connection-parameters-set! conn args)
                             (amqp-send channel 1
                                        (amqp:make-connection-tune-ok (alist-ref 'channel-max args)
                                                                      (alist-ref 'frame-max args)
                                                                      (alist-ref 'heartbeat args)))
                             (amqp-send channel 1
                                        (amqp:make-connection-open "/"))))
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

(define (default-0) 0)

(define (channel-open conn)
  (let* ((id (next-channel-id conn))
         (mb (dispatch-register! conn `((channel-id . ,id) (type . 1))))
         (ch (make-channel id mb conn)))
    (amqp-send ch 1 (amqp:make-channel-open))
    (amqp-expect ch 20 11)
    ch))

(define (queue-declare channel queue #!key (passive 0) (durable 0) (exclusive 0) (auto-delete 0) (no-wait 0))
  (amqp-send channel 1 (amqp:make-queue-declare queue passive durable exclusive auto-delete no-wait '()))
  (let ((reply (amqp-expect channel 50 11)))
    (alist-ref 'queue (frame-properties reply))))

(define (queue-bind channel queue exchange routing-key . args)
  (amqp-send channel 1 (amqp:make-queue-bind queue exchange routing-key
                                             (get-keyword no-wait: args default-0)
                                             '()))
  (amqp-expect channel 50 21))

(define (consume channel queue . flags)
  (let ((tag "2abc"))
  (amqp-send channel 1 (amqp:make-basic-consume queue tag
                                                (get-keyword no-local: flags default-0)
                                                (get-keyword no-ack: flags default-0)
                                                (get-keyword exclusive: flags default-0)
                                                (get-keyword no-wait: flags default-0)
                                                '()))
  (amqp-expect channel 60 21)))
