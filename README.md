## [module] amqp
## Description

A low-level [Chicken Scheme](https://call-cc.org) client for the
Advanced Message Queueing Protocol v0.9.1.

### Limitations

* Only AMQP 0.9.1 is supported, and 1.0 support is unlikely to
be added.
* Only PLAIN authentication is supported.
* No SSL support.

## Authors

Fredrik Appelberg (fredrik@appelberg.me)

## Repository

https://github.com/fred-o/chicken-amqp

## Requirements

* [[srfi-18]]
* [[mailbox]]
* [[bitstring]]
* [[uri-generic]]

## Examples

### Producer

```Scheme
;; Establish a connection and open a channel
(define conn (amqp-connect "amqp://myuser:mypassword@myserver/myvhost"))
(define chan (amqp-channel-open conn))

;; Declare a durable topic exchange
(amqp-exchange-declare chan "my-exchange" "topic" durable: 1)

;; Publish a message with a plain text payload
(amqp-publish-message chan "my-exchange" "my-routing-key" "hello, world" '((content-type "text/plain")))
```

### Consumer

```Scheme
;; Establish a connection and open a channel
(define conn (amqp-connect "amqp://myuser:mypassword@myserver/myvhost"))
(define chan (amqp-channel-open conn))

;; Declare the previously created exchange passively. Not strictly
;; neccessary, but good practice.
(amqp-exchange-declare chan "my-exchange" passive: 1)

;; Declare an anymous queue. The server will assign a random name, so
;; we need to check the return data to get it. 
(let ((q (alist-ref 'queue (amqp-declare-queue chan "" auto-delete: 1))))
  ;; Bind the queue to our exchange
  (amqp-queue-bind chan q "my-exchange" "#")

  ;; Tell the server that we want to start consuming messages
  (amqp-basic-consume chan q)

  ;; Loop forever, recieving messages and printing the payload
  (let loop ()
    (let ((msg (amqp-receive-message chan))
      (print (blob->string (amqp-message-payload msg)))
      (loop)))))
```

## API

### Concurrency

Each AMQP connection starts two SRFI-18 threads; one for reading
and dispatching incoming frames, and one for handling heartbeats.

All operations use SRFI-18 mutexes to ensure thread safety, and
it should be perfectly fine to have multiple threads accessing
the same channel. The `mailbox` egg is used internally when data
needs to be passed between threads.

### Conditions

AMQP handles errors by simply closing down the offending channel
and/or connection. When this happens an *amqp* condition with
`reply-text` and `reply-code` properties is raised, and the
channel/connection object becomes invalid.

### Connections

#### [procedure] `(amqp-connect URL)`
  
Create a new AMQP connection and returns a connection object.
The `url` parameter should have the format `amqp://[user][:password@][host]/[vhost]`  


#### [procedure] `(amqp-disconnect CONNECTION)`
  
Close an AMQP connection.   


#### [variable] `amqp-debug`  
**default:** `(make-parameter #f)`  
Set `amqp-debug` to `#t` to enable detailed debug logging of
sent and received frames.  


### Receiving messages

#### [procedure] `(amqp-receive-message CHANNEL)`
  
Block until the next `amqp-message` can be read on the given CHANNEL.

The client will receive messages from any of the following AMQP methods:
* `basic.deliver` (after `basic.consume` on the same channel)
* `basic.return` (for messages sent with `mandatory: 1` or `immediate: 1` that could not be
routed or delivered immediately by the server)
* `basic.get-ok` (in response to `basic.get`)  


### [record] `amqp-message`  
**[constructor] `(make-amqp-message DELIVERY PROPERTIES PAYLOAD)`**  
**[predicate] `amqp-message?`**  
**implementation:** `define-record`  

field        | getter                    | setter                        
------------ | ------------------------- | ------------------------------
`delivery`   | `amqp-message-delivery`   | `amqp-message-delivery-set!`  
`properties` | `amqp-message-properties` | `amqp-message-properties-set!`
`payload`    | `amqp-message-payload`    | `amqp-message-payload-set!`   
  
Record that holds received messages. The `delivery` slot holds
an alist of delivery information from the server. The
`properties` slot is an alist of AMQP message properties.  


#### [variable] `amqp-payload-conversion`  
**default:** `(make-parameter bitstring->blob)`  
The `ampq` egg uses `bitstring` to handle binary data internally,
but in an attempt to avoid abstraction leakage message payloads
returned by `amqp-receive-message` are converted to standard
`blob`s. If you don't want this behaviour, set the parameter
`amqp-payload-conversion` to either #f or a function that
accepts a `bitstring` and returns the desired payload format.  


### Publishing messages

#### [procedure] `(amqp-publish-message CHANNEL EXCHANGE ROUTING-KEY PAYLOAD PROPERTIES #!key (MANDATORY 0) (IMMEDIATE 0))`
  
Publish a message. The PAYLOAD can be an `u8vector`, `string`,
`vector` or `blob`. PROPERTIES should be an alist of AMQP
message properties.  


### AMQP Commands

The AMQP command functions map 1:1 to the commands specified by
the AMQP specification.

* The first argument to any command function except
`amqp-channel-open` should be a `channel` object. For other
arguments, consult the AMQP specification.
* All synchronous command functions except `amqp-channel-open` return an alist of values sent
by the server in the reply command. Consult the AMQP specification for details.
* Asynchronous command functions (`amqp-basic-ack`, `amqp-basic-reject`,
etc.) or synchronous functions called with `no-wait: 1` return `(void)`.
* `amqp-channel-open` returns a new `channel` object.

#### The Channel Class

#### [procedure] `(amqp-channel-open CONNECTION)`
  
Returns a new `channel` object that is then passed as an
argument to the other functions.  


#### [procedure] `(amqp-channel-flow CHANNEL ACTIVE)`
  


#### [procedure] `(amqp-channel-close CHANNEL #!key (REPLY-CODE 0) (REPLY-TEXT "") (METHOD-ID 0) (CLASS-ID 0))`
  


#### [procedure] `(amqp-exchange-declare CHANNEL EXCHANGE TYPE #!key (PASSIVE 0) (DURABLE 0) (NO-WAIT 0) (ARGUMENTS '()))`
  
#### The Exchange Class  


#### [procedure] `(amqp-exchange-delete CHANNEL EXCHANGE #!key (IF-UNUSED 0) (NO-WAIT 0))`
  


#### [procedure] `(amqp-queue-declare CHANNEL QUEUE #!key (PASSIVE 0) (DURABLE 0) (EXCLUSIVE 0) (AUTO-DELETE 0) (NO-WAIT 0) (ARGUMENTS '()))`
  
#### The Queue Class  


#### [procedure] `(amqp-queue-bind CHANNEL QUEUE EXCHANGE ROUTING-KEY #!key (NO-WAIT 0) (ARGUMENTS '()))`
  


#### [procedure] `(amqp-queue-purge CHANNEL QUEUE #!key (NO-WAIT 0))`
  


#### [procedure] `(amqp-queue-delete CHANNEL QUEUE #!key (IF-UNUSED 0) (IF-EMPTY 0) (NO-WAIT 0))`
  


#### [procedure] `(amqp-queue-unbind CHANNEL QUEUE EXCHANGE ROUTING-KEY #!key (ARGUMENTS '()))`
  


#### [procedure] `(amqp-basic-qos CHANNEL PREFETCH-SIZE PREFETCH-COUNT GLOBAL)`
  
#### The Basic Class  


#### [procedure] `(amqp-basic-consume CHANNEL QUEUE #!key (CONSUMER-TAG "") (NO-LOCAL 0) (NO-ACK 0) (EXCLUSIVE 0) (NO-WAIT 0) (ARGUMENTS '()))`
  


#### [procedure] `(amqp-basic-cancel CHANNEL CONSUMER-TAG #!key (NO-WAIT 0))`
  


#### [procedure] `(amqp-basic-get CHANNEL QUEUE #!key (NO-ACK 0))`
  


#### [procedure] `(amqp-basic-ack CHANNEL DELIVERY-TAG #!key (MULTIPLE 0))`
  


#### [procedure] `(amqp-basic-reject CHANNEL DELIVERY-TAG #!key (REQUEUE 0))`
  


#### [procedure] `(amqp-basic-recover CHANNEL REQUEUE)`
  


#### [procedure] `(amqp-basic-recover-async CHANNEL REQUEUE)`
  


#### [procedure] `(amqp-tx-select CHANNEL)`
  
#### The Transaction Class  


#### [procedure] `(amqp-tx-commit CHANNEL)`
  


#### [procedure] `(amqp-tx-rollback CHANNEL)`
  


## License

BSD

## Version History

* 0.9 - Preparing for an 1.0 release
