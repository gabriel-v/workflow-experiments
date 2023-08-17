from qbase import init, submit_task, TEST_Q, TEST_Q_2, fetch_result, wait_until_notified
import demo

init()
submit_task(TEST_Q, demo.somefun, 'penis')
import time
wait_until_notified(TEST_Q_2)
while True:
    with fetch_result(TEST_Q_2) as result:
        if result is None:
            break
        print(result)
