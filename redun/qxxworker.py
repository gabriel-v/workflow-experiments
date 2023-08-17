from qbase import init, run_worker_forever, QUEUE_SEND, QUEUE_RECV

import w
import qexecutor

init()
run_worker_forever(QUEUE_SEND, QUEUE_RECV)
