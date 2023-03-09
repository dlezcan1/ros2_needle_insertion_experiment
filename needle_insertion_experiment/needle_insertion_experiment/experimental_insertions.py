import time
import rclpy

from rclpy.action import ActionClient
from rclpy.node import Node
from rclpy.parameter import Parameter

from std_msgs.msg import Bool, Float32

from needle_insertion_robot_translation_interfaces.action import MoveStage

class InsertionExperimentControllerNode(Node):
    X_AXIS_NAME  = 'x'
    Y_AXIS_NAME  = 'y'
    LS_AXIS_NAME = 'ls'

    def __init__(self, name="InsertionExperimentControllerNode"):
        super().__init__( name )

        # current parameters
        self._current_insdepth_idx = 0
        self.ax_x_pos, self.ax_y_pos, self.ax_ls_pos = 0, 0, 0

        # ROS parameters
        # - communication
        robot_ns = self.declare_parameter("robot.ns", value="")

        # - experimental
        self.insertion_depths = self.declare_parameter( "insertion.depths" ).get_parameter_value().double_array_value
        assert len(self.insertion_depths) > 0, "Need insertion depths!"

        self.ax_x_max = self.declare_parameter("insertion.x.max", value=35).get_parameter_value().double_value
        assert self.ax_x_max > 0, f"X-axis max must be > 0. {self.ax_x_max} <= 0!"

        self.y_increment = self.declare_parameter( "lateral.increment" ).get_parameter_value().double_value

        # subscriptions
        self.sub_x_pos  = self.create_subscription(
            Float32, 
            f"{robot_ns}/axis/position/x",
            lambda msg: self.sub_position_callback(msg, self.X_AXIS_NAME)
        )
        self.sub_y_pos  = self.create_subscription(
            Float32, 
            f"{robot_ns}/axis/position/y",
            lambda msg: self.sub_position_callback(msg, self.Y_AXIS_NAME)
        )
        self.sub_ls_pos = self.create_subscription(
            Float32, 
            f"{robot_ns}/axis/position/linear_stage",
            lambda msg: self.sub_position_callback(msg, self.LS_AXIS_NAME)
        )

        # publishers
        self.pub_x_cmd = self.create_publisher(
            Float32,
            f"{robot_ns}/axis/command/absolute/x",
            10,
        )
        self.pub_y_cmd = self.create_publisher(
            Float32,
            f"{robot_ns}/axis/command/relative/y",
            10,
        )
        self.pub_ls_cmd = self.create_publisher(
            Float32,
            f"{robot_ns}/axis/command/absolute/linear_stage",
            10,
        )
        
    # __init__

    def command_insertion_depth(self, depth):
        """ Command the robot to a specified insertion depth """
        x_ax_pos  = min(depth, self.ax_x_max)
        ls_ax_pos = depth - x_ax_pos

        self.get_logger().info(
            f"Commanding insertion depth to {depth} mm | X->{x_ax_pos} mm and LS->{ls_ax_pos} mm"
        )
        self.pub_x_cmd.publish(Float32(data=x_ax_pos))
        self.pub_ls_cmd.publish(Float32(data=ls_ax_pos)) 

    # command_insertion_depth

    def command_next_insertion_depth(self):
        """ Command to the next insertion depth """
        self._current_insdepth_idx += 1

        if self._current_insdepth_idx >= len(self.insertion_depths):
            self.get_logger().info("Beginning new insertion trial!")
            self.increment_y_axis()
            self._current_insdepth_idx = 0
        
        # if

        self.command_insertion_depth(self.insertion_depths[self._current_insdepth_idx])
        if self._current_insdepth_idx < len(self.insertion_depths) - 1:
            self.get_logger().info(
                f"Next insertion depth is: {self.insertion_depths[self._current_insdepth_idx + 1]} mm"
            )

        else:
            self.get_logger.info(
                f"Next step is a new insertion trial!"
            )

    # def

    def handle_keyinput(self, keyinput: str):
        if self.isfloat(keyinput):
            self.command_insertion_depth(keyinput)

        elif keyinput.lower() == "reset":
            self.reset_insertion()

        else:
            self.command_next_insertion_depth()

    # handle_keyinput

    def increment_y_axis(self):
        self.get_logger().info(f"Incrementing y-axis for {self.y_increment} mm")
        self.pub_y_cmd.publish(Float32(data=self.y_increment))

    # increment_y_axis

    def reset_insertion(self):
        self.get_logger().warn("Resetting insertion!")
        self._current_insdepth_idx = 0
        self.command_insertion_depth(self.insertion_depths[0])

    def sub_position_callback(self, msg: Float32, axis: str):
        if axis ==  self.X_AXIS_NAME:
            self.ax_x_pos = float(msg.data)

        elif axis ==  self.Y_AXIS_NAME:
            self.ax_y_pos = float(msg.data)

        elif axis == self.LS_AXIS_NAME:
            self.ax_ls_pos = float(msg.data)

    # sub_position_callback

    @staticmethod
    def isfloat(s: str):
        try:
            float(s)
            return True
        except ValueError:
            return False
        
    # isfloat

# class: InsertionExperimentControllerNode

def main(args=None):
    rclpy.init(args=args)

    ins_expmt_ctrl_node = InsertionExperimentControllerNode()

    try:
        while True:
            break

    finally:
        ins_expmt_ctrl_node.destroy_node()

    rclpy.shutdown()

# main

if __name__ == "__main__":
    main()

# if __main__