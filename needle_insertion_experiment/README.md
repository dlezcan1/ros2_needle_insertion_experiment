## Running insertion experiment controller node

Run the needle insertion experiment controller node
```bash 
ros2 run needle_insertion_experiment experiment_controller --ros-args \
    -p robot.ns:=/stage \
    -p insertion.depths:="[ 0., 35.,  65., 95., 125. ]" \
    -p lateral.increment:=10. \
    -p insertion.retraction_distance:=-5.0
```