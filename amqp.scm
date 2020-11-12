(module amqp ()
  (import amqp-core amqp-primitives (chicken module))
  (reexport (only amqp-core
				  amqp-connect
				  amqp-disconnect
				  amqp-debug))
  (reexport (except amqp-primitives
					make-uuid-v4
					with-lock)))
