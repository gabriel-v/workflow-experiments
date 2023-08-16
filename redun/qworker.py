from qbase import init, run_worker_forever, TEST_Q, TEST_Q_2

import demo

init()
run_worker_forever(TEST_Q, TEST_Q_2)
