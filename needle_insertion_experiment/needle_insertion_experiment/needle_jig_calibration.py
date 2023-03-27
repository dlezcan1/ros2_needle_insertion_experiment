import os
import multiprocessing
from datetime import datetime

import numpy as np

import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor

from std_msgs.msg import Float64, Float64MultiArray

from . import utilities

class JigCalibrationStateNode(Node):
    def __init__(self, name="JigCalibrationStateNode"):
        super().__init__(name)

        # fields
        self._signals          = list() # [(timestamp, np.array of signals)]
        self.current_curvature = 0.0 # 1/m
        self.current_angle     = 0.0 # deg
        self.gather_data       = False

        # publishers
        self.pub_curvature = self.create_publisher(
            Float64,
            'jig/curvature',
            10,
        )
        self.pub_angle = self.create_publisher(
            Float64,
            'jig/needle_angle',
            10,
        )

        # subscribers
        self.sub_signals = self.create_subscription(
            Float64MultiArray,
            'sensor/raw',
            self.callback_sub_signals,
            10,
        )

        self.timer_state = self.create_timer(
            1,
            self.publish_state,
        )

    # __init__

    def callback_sub_signals(self, msg: Float64MultiArray):
        """ Callback for subscribing to signals"""
        if not self.gather_data:
            return 
        
        self._signals.append((
            datetime.now().strftime(JigCalibratorNode.DATETIME_FMT),
            np.asarray(msg.data, dtype=np.float64))
        )

    # callback_sub_signals

    def log_state(self):
        self.get_logger().info(f"Current state: {self.current_angle} degs - {self.current_curvature} 1/m")

    # log_state

    def publish_state(self):
        msg_curv = Float64(data=self.current_curvature)
        msg_angl = Float64(data=self.current_angle)

        self.pub_curvature.publish(msg_curv)
        self.pub_angle.publish(msg_angl)

    # publish_state

    def update_state(self, curvature: float = None, angle: float = None):
        if curvature is not None:
            self.current_curvature = float(curvature)

        if angle is not None:
            self.current_angle = float(angle)

        self.log_state()

    # update_state

# class: JigCalibrationStateNode

class JigCalibratorNode(Node):
    CALIB_CURVATURES = [0.0, 0.5, 1.6, 2.0, 2.5, 3.2, 4.0]
    DATETIME_FMT = "%Y-%m-%d_%H-%M-%S"

    def __init__(self, name="JigCalibratorNode"):
        super().__init__(name)
        
        # fields
        self.executor = None
        
        # - state counter
        self._trial_counter = 0
        self._curvt_counter = 0
        self._angle_counter = 0

        # parameters
        self.num_trials = self.declare_parameter(
            "calibration.trials", value=5,
        ).get_parameter_value().integer_value

        self.num_signals = self.declare_parameter(
            "sensor.num_signals", value=200
        ).get_parameter_value().integer_value

        self.curvatures = self.declare_parameter(
            "calibration.curvatures", value=self.CALIB_CURVATURES
        ).get_parameter_value().double_array_value

        self.angles = self.declare_parameter(
            "calibration.angles", value=[0.0, 90.0]
        ).get_parameter_value().double_array_value

        self.outdir = self.declare_parameter(
            "calibration.out_dir", value=".",
        ).get_parameter_value().string_value

        # helper node
        self.node_state = JigCalibrationStateNode()

        # timers
        self.timer_input = self.create_timer(
            5,
            self.handle_state,
        )

        self.node_state.update_state(
            curvature=self.curvatures[self._curvt_counter],
            angle=self.angles[self._angle_counter],
        )

    # __init__

    def handle_input(self, inp: str):
        if len(inp.split(",")) == 2:
            curv, angle = inp.split(',')
            assert utilities.isfloat(curv), f"'{curv}' is not a float"
            assert utilities.isfloat(angle), f"'{angle}' is not a float"

            curv  = float(curv)
            angle = float(angle)

            self.node_state.update_state(curvature=curv, angle=angle)
            self._trial_counter = 0 # reset trial counter

        # if

        elif inp == "":
            self.handle_current_state()

        # elif

        elif inp.lower() == "quit":
            return False
        
        else:
            self.get_logger().warn(f"'{inp}' is not a valid command!")

        return True

    # handle_input

    def handle_current_state(self):
        """ Handle calibration """
        self.node_state._signals = list()
        self.node_state.gather_data = True

        while len(self.node_state._signals) < self.num_signals:
            self.executor.spin_once(timeout_sec=1.5)
            self.get_logger().info(f"Signals collected: {len(self.node_state._signals)}/{self.num_signals}")

        # while
        self.node_state.gather_data = False

        self.save_trial(file_append=None)
        
        self.update_to_next_state()


    # handle_current_state

    def handle_state(self):
        self.node_state.log_state()
        commands = (
            "[Jig Calibration] Commands:"
            "\n".join(
                map(
                    lambda s: f"\t- {s}",
                    [
                        "[ENTER] to command to next insertion trial",
                        "'quit' to quit this calibration",
                        "<float for curvature>, <float for angle> to set curvature and angle",
                    ]    
                )
            )
        )
        print(commands)
        inp = input("Input: ")
        cnt_calibration = self.handle_input(inp)
        
        if not cnt_calibration:
            rclpy.try_shutdown()

        # if

    # handle_state

    def save_trial(self, file_append: str = None):
        if file_append is None:
            file_append = datetime.now().strftime(JigCalibratorNode.DATETIME_FMT)

        # if

        path = os.path.join(
            self.outdir,
            f"{self.node_state.current_angle}_deg",
            f"{self.node_state.current_curvature:.1f}",
            f"fbgdata_{file_append}.txt"
        )

        os.makedirs(os.path.split(path)[0], exist_ok=True)

        with open(path, 'w') as fileout:
            for ts, signals in self.node_state._signals:
                fileout.write(
                    f"{ts}: "
                    + np.array2string(
                        signals, 
                        precision=10, 
                        separator=', ',
                        max_line_width=np.inf,
                    ).strip('[]')
                    + '\n'
                )

            # for
        # with

        self.get_logger().info(f"Saved FBG data trial to: {path}")

    # save_trial

    def spin(self, executor: MultiThreadedExecutor=None):
        if executor is None:
            executor = MultiThreadedExecutor(num_threads=multiprocessing.cpu_count())

        # if

        self.executor = executor

        self.executor.add_node(self.node_state)
        self.executor.add_node(self)

        self.executor.spin()

    # spin

    def update_to_next_state(self):
        """ Update to the next state"""
        next_curvature = None
        next_angle     = None
        
        self._trial_counter += 1
        

        if self._trial_counter < self.num_trials:
            return

        self._curvt_counter += 1
        self._trial_counter = 0
        
        if self._curvt_counter >= len(self.curvatures):
            self._angle_counter += 1
            self._curvt_counter = 0

        # if

        if self._angle_counter >= len(self.angles):
            self.get_logger().info("Completed insertion experiment. Shutting down")
            self.executor.shutdown()

        # if

        next_curvature = self.curvatures[self._curvt_counter]
        next_angle     = self.angles[self._angle_counter]

        self.node_state.update_state(
            curvature=next_curvature,
            angle=next_angle,
        )

    # update_to_next_state

# class JigCalibratorNode

def main(args=None):
    rclpy.init(args=args)

    jig_calibrator_node = JigCalibratorNode()

    try:
        jig_calibrator_node.spin()

    except KeyboardInterrupt:
        pass

    rclpy.try_shutdown()


# main

if __name__ == "__main__":
    main()

# if __main__