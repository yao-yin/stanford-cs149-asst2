#!/bin/bash
./runtasks -n 8 ping_pong_equal
./runtasks -n 8 ping_pong_unequal
./runtasks -n 8 super_light
./runtasks -n 8 super_super_light
./runtasks -n 8 recursive_fibonacci
./runtasks -n 8 math_operations_in_tight_for_loop
./runtasks -n 8 math_operations_in_tight_for_loop_fewer_tasks
./runtasks -n 8 math_operations_in_tight_for_loop_fan_in
./runtasks -n 8 math_operations_in_tight_for_loop_reduction_tree
./runtasks -n 8 spin_between_run_calls
./runtasks -n 8 mandelbrot_chunked
