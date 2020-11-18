(import amqp-091
		test)

(test-group "frame-match?"
  (test "class & method" #t (frame-match? (make-frame 1 #f 20 40 0 #f #f)
										  #f 20 40))
  (test "type & class & method" #t (frame-match? (make-frame 1 #f 20 40 0 #f #f)
												 1 20 40))
  (test "list of methods" #t (frame-match? (make-frame 1 #f 20 40 0 #f #f)
										  #f 20 '(30 40))))

(test-end)
