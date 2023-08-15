#!/bin/bash -ex
ray job submit --address="http://127.0.0.1:8265" -- python w.py
