import time
import rclpy

from rclpy.action import ActionClient
from rclpy.node import Node, Publisher
from rclpy.parameter import Parameter

from std_msgs.msg import Bool, Float32

from . import utilities

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
        self.ax_x_moving, self.ax_y_moving, self.ax_ls_moving = False, False, False

        # ROS parameters
        # - communication
        robot_ns = self.declare_parameter("robot.ns", value="").get_parameter_value().string_value

        # - experimental
        self.insertion_depths = self.declare_parameter( "insertion.depths" ).get_parameter_value().double_array_value
        assert len(self.insertion_depths) > 0, "Need insertion depths!"

        self.ax_x_max = self.declare_parameter("insertion.x.max", value=35.0).get_parameter_value().double_value
        assert self.ax_x_max > 0, f"X-axis max must be > 0. {self.ax_x_max} <= 0!"

        self.y_increment = self.declare_parameter( "lateral.increment" ).get_parameter_value().double_value
        
        # action client
        self.actionclient_robot = ActionClient(
            self,
            MoveStage,
            f"{robot_ns}/move_stage"
        )

        # subscriptions
        self.sub_x_pos  = self.create_subscription(
            Float32, 
            f"{robot_ns}/axis/position/x",
            lambda msg: self.sub_position_callback(msg, self.X_AXIS_NAME),
            10,
        )
        self.sub_y_pos  = self.create_subscription(
            Float32, 
            f"{robot_ns}/axis/position/y",
            lambda msg: self.sub_position_callback(msg, self.Y_AXIS_NAME),
            10,
        )
        self.sub_ls_pos = self.create_subscription(
            Float32, 
            f"{robot_ns}/axis/position/linear_stage",
            lambda msg: self.sub_position_callback(msg, self.LS_AXIS_NAME),
            10,
        )

        self.sub_x_moving  = self.create_subscription(
            Bool, 
            f"{robot_ns}/axis/moving/x",
            lambda msg: self.sub_moving_callback(msg, self.X_AXIS_NAME),
            10,
        )
        self.sub_y_moving  = self.create_subscription(
            Bool, 
            f"{robot_ns}/axis/moving/y",
            lambda msg: self.sub_moving_callback(msg, self.Y_AXIS_NAME),
            10,
        )
        self.sub_ls_moving = self.create_subscription(
            Bool, 
            f"{robot_ns}/axis/moving/linear_stage",
            lambda msg: self.sub_moving_callback(msg, self.LS_AXIS_NAME),
            10,
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

    def command_robot(self, x=None, y=None, ls=None):
        goal_msg = MoveStage.Goal()
        goal_msg.eps = 1e-4

        goal_msg.move_x            = False
        goal_msg.move_y            = False
        goal_msg.move_z            = False
        goal_msg.move_linear_stage = False

        if x is not None:
            goal_msg.x = x
            goal_msg.move_x = True

        # if
        if y is not None:
            goal_msg.y = y
            goal_msg.move_y = True

        # if
        if ls is not None:
            goal_msg.linear_stage = ls
            goal_msg.move_linear_stage = True

        # if
        
        future = self.actionclient_robot.send_goal_async(goal_msg)

        rclpy.spin_until_future_complete(self.actionclient_robot, future)

    # command_robot

    def command_insertion_depth(self, depth):
        """ Command the robot to a specified insertion depth """
        x_ax_pos  = min(depth, self.ax_x_max)
        ls_ax_pos = depth - x_ax_pos

        self.get_logger().info(
            f"Commanding insertion depth to {depth} mm | X->{x_ax_pos} mm and LS->{ls_ax_pos} mm"
        )
        self.command_robot(x=x_ax_pos, ls=ls_ax_pos)

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
            self.get_logger().info(
                f"Next step is a new insertion trial!"
            )

    # def

    def handle_keyinput(self, keyinput: str):
        if utilities.isfloat(keyinput):
            self.command_insertion_depth(float(keyinput))

        elif keyinput.lower() == "reset":
            self.reset_insertion()
        
        elif keyinput.lower() == "quit":
            return False
        
        elif keyinput == "":
            self.command_next_insertion_depth()

        else:
            self.get_logger().warn(f"'{keyinput}' is not a valid command!")

        return True

    # handle_keyinput

    def increment_y_axis(self):
        self.get_logger().info(f"Incrementing y-axis for {self.y_increment} mm")
        # self.pub_y_cmd.publish(Float32(data=self.y_increment))
        self.command_robot(y=self.ax_y_pos + self.y_increment)

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

# class: InsertionExperimentControllerNode

def main(args=None):
    rclpy.init(args=args)

    ins_expmt_ctrl_node = InsertionExperimentControllerNode()

    try:
        continue_expmt = True
        while continue_expmt:
            print(
                "Commands:\n"
                + "\n".join(
                    map(
                        lambda s: f"\t- {s}",
                        [
                            "'reset' to reset insertion trial",
                            "Floating point number to command to specified insertion depth",
                            "'quit' to quit this controller",
                            "[ENTER] to command to next insertion trial"
                        ]
                    )
                )
            )
            key_inp = input("Input: ")
            rclpy.spin_once(ins_expmt_ctrl_node, timeout_sec=5)
            continue_expmt = ins_expmt_ctrl_node.handle_keyinput(key_inp)
            rclpy.spin_once(ins_expmt_ctrl_node, timeout_sec=5)
            print()

    finally:
        ins_expmt_ctrl_node.destroy_node()

    rclpy.shutdown()

# main

if __name__ == "__main__":
    main()

# if __main__