from abc import ABC, abstractmethod, abstractproperty
from collections import defaultdict
import itertools
import os
from typing import List, Dict, Tuple
import warnings

import numpy as np
import pandas as pd

from std_msgs.msg import Float64MultiArray
from geometry_msgs.msg import (
    PoseStamped, 
    PoseArray,
)

from .bag_file_parser import BagFileParser
from needle_shape_publisher.utilites import (
    msg2pose,
    msg2poses,
)


class ExperimentBagParser(ABC, BagFileParser):
    def __init__(
            self, 
            bagdir: str, 
            insertion_depths: List[float], 
            bagfile: str = None, 
            yamlfile: str = None, 
            topics: List[str] = None,
        ):
        super().__init__(
            bagdir, 
            bagfile=bagfile, 
            yamlfile=yamlfile,
        )
        self.topics_of_interest = topics
        self.insertion_depths = sorted(insertion_depths)

    # __init__

    @abstractmethod
    def parse_data(self):
        """ Parse the Insertion Experiment Data """
        pass

    # parse_data

    @abstractmethod
    def save_results(self, odir: str, filename: str = None):
        """ Save the results to the output directory """
        pass

    # save_results

# class: ExperimentBagParser

class TimestampDependentExperimentBagParser(ExperimentBagParser, ABC):
    def __init__(
            self, 
            bagdir: str, 
            insertion_depths: List[float], 
            bagfile: str = None, 
            yamlfile: str = None, 
            topics: List[str] = None
        ):
        super().__init__(bagdir, insertion_depths, bagfile, yamlfile, topics)

        self.timestamp_ranges  = None
        self.target_timestamps = None
        self.timestamps        = dict()

    # __init__

    def configure_timestamp_ranges(self, timestamp_ranges: Dict[float, Tuple[int, int]]):
        """ Set the min and max timestamp ranges for each of the insertion depths """
        assert set(timestamp_ranges.keys()).issubset(self.insertion_depths), (
            "Configured timestamp ranges has insertion depths not setup to be processed: "
            f"{set(timestamp_ranges.keys()).difference(self.insertion_depths)}"
        )

        self.timestamp_ranges = timestamp_ranges

    # configure_timestamp_ranges

    def configure_target_timestamps(self, target_ts: Dict[float, int]):
        assert set(target_ts.keys()).issubset(self.insertion_depths), (
            "Configured target timestamps has insertion depths not setup to be processed: "
            f"{set(target_ts.keys()).difference(self.insertion_depths)}"
        )

        self.target_timestamps = target_ts

    # configure_target_timestamps

    @abstractmethod
    def determine_timestamps(self, inplace: bool = False):
        """ Method to determine which timestamps to use.
            Should return timestamps and fill self.timestamps (if inplace is True)
        """
        pass

    # determine_timestamps

    def determine_timestamps_closest_to_target(
        self,
        by_topic: str,
        target_timestamps: Dict[float, int],
        ts_range: tuple = None,
        ts_range_exclude: tuple = None,
        inplace: bool = False
    ):
        """ Determines which timestamps to use based on a topic
            which has timestamps closest to the target timestamps
            by which topic related
        """
        timestamps = dict()
        if inplace:
            timestamps = self.timestamps

        for depth, target_ts in target_timestamps.items():
            ts_range_d = ts_range
            if ts_range_d is None and self.timestamp_ranges is not None:
                ts_range_d = self.timestamp_ranges.get(depth, (None, None))

            closest_ts, _ = self.get_closest_message_to_timestamp(
                by_topic, 
                target_ts, 
                ts_range=ts_range_d,
                ts_range_exclude=ts_range_exclude,
            )

            timestamps[depth] = closest_ts

        # for

        return timestamps
    
    # determine_timestamps_closest_to_target

# class: TimestampDependentExperimentBagParser
    

class CameraDataBagParser(TimestampDependentExperimentBagParser):
    DEFAULT_TOPICS_OF_INTEREST = None # TODO

    def __init__(
            self, 
            bagdir: str, 
            insertion_depths: List[float], 
            bagfile: str = None, 
            yamlfile: str = None, 
            topics: List[str] = None,
        ):
        super().__init__(bagdir, insertion_depths, bagfile, yamlfile, topics)

    # __init__

    def determine_timestamps(self, inplace: bool = False):
        assert self.target_timestamps is not None, (
            "You must configure the target timestamps for each of the insertion depths"
        )
        

        timestamps = self.determine_timestamps_closest_to_target(
            key_topic,
            self.target_timestamps,
            ts_range=self.timestamp_ranges
        )

        return timestamps
    
    # determine_timestamps

# class: CameraDataBagParser

class RobotDataBagParser(ExperimentBagParser):
    DEFAULT_TOPICS_OF_INTEREST = ["/stage/state/needle_pose"] + [ 
        "/".join([ns, topic, axis]) 
        for ns, topic, axis in
        itertools.product(
            ["/stage/axis"],
            ["command", "position", "state/moving", "state/on"],
            ["linear_stage", "x", "y", "z"]
        )
    ]

    def __init__(
            self, 
            bagdir: str, 
            insertion_depths: List[float], 
            bagfile: str = None, 
            yamlfile: str = None, 
            topics: List[str] = None,
        ):
        super().__init__(
            bagdir, 
            insertion_depths=insertion_depths,
            bagfile=bagfile, 
            yamlfile=yamlfile, 
            topics=topics if topics is not None else RobotDataBagParser.DEFAULT_TOPICS_OF_INTEREST
        )

        self.key_topic = list(filter(lambda t_name: "state/needle_pose" in t_name, self.topics_of_interest))[0]

        # data members (from parsing the bag)
        self.insertion_depth_timestamp_ranges = None

    # __init__

    def parse_data(self):
        bag_rows = self.get_messages(
            topic_name=self.key_topic,
            generator_count=1,
        )

        insertion_depth_timestamp_ranges = defaultdict(list)

        for i, (ts, topic, msg) in enumerate(bag_rows):
            insertion_depth = self.get_insertion_depth(msg)

            for d in self.insertion_depths:
                if abs(d - insertion_depth) < 1e-6:
                    insertion_depth_timestamp_ranges[d].append(ts)
                    break
                # if
            # for

        # for
            
        self.insertion_depth_timestamp_ranges = {
            d: ( min(timestamps), max(timestamps) )
            for d, timestamps in insertion_depth_timestamp_ranges.items()
        }

        return self.insertion_depth_timestamp_ranges
    
    # parse_data

    def save_results(self, odir: str, filename: str = None):
        assert self.insertion_depth_timestamp_ranges is not None, (
            "Robot data has not been parsed yet."
        )
        filename = filename if filename is not None else "robot_timestamp_ranges.csv"

        ts_ranges_df = pd.DataFrame.from_dict(
            self.insertion_depth_timestamp_ranges,
            orient='index',
            columns=['ts_min', 'ts_max'],
        )

        os.makedirs(odir, exist_ok=True)
        ts_ranges_df.to_csv(
            os.path.join(odir, filename),
            header=True,
            index=True,
            index_label="Insertion Depth (mm)"
        )
        print(
            "Saved robot timestamp data to:", 
            os.path.join(odir, filename)
        )

    # save_results

    @classmethod
    def get_insertion_depth(cls, needle_pose_msg: PoseStamped):
        """ Returns the insertion depth of the state/needle_pose topic message """ 
        return needle_pose_msg.pose.position.z
    
    # get_insertion_depth

# class: RobotDataBagParser

class NeedleDataBagParser(TimestampDependentExperimentBagParser):
    DEFAULT_TOPICS_OF_INTEREST = [
        "/needle/sensor/raw",
        "/needle/sensor/processed",
        "/needle/state/current_shape",
        "/needle/state/kappac",
        "/needle/state/winit",
        "/needle/state/skin_entry",
        "/stage/state/needle_pose",
    ]

    def __init__(
            self, 
            bagdir: str, 
            insertion_depths: List[float], 
            bagfile: str = None, 
            yamlfile: str = None, 
            topics: List[str] = None,
        ):
        super().__init__(
            bagdir, 
            insertion_depth=insertion_depths,
            bagfile=bagfile, 
            yamlfile=yamlfile, 
            topics=topics if topics is not None else NeedleDataBagParser.DEFAULT_TOPICS_OF_INTEREST
        )

        # parsed data results
        # self.needle_data = {
        #   insertion depth: { 
        #       data: {
        #           kappa_c: (N, )  floats, 
        #           winit:   (N, )  floats,
        #           shape:   (N, 3) floats,
        #       } 
        #       timestamps: {
        #           kappa_c: ts of kappac topic
        #           winit:   ts of winit topic
        #           shape:   ts of shape topic
        #       }
        #   }
        # }
        self.needle_data = defaultdict(
            lambda: {
                "data": dict(),
                "timestamps": dict(),
            },
        )

    # __init__

    def determine_timestamps(self, inplace: bool=False):
        timestamps = dict()
        if inplace:
            timestamps = self.timestamps

        kappac_topic = list(filter(lambda topic: "state/kappac" in topic))[0]
        for depth, ts_range in self.timestamp_ranges.items():
            kappac_msgs = self.get_messages(kappac_topic, ts_range=ts_range, generator_count=-1)

            viable_ts_msg = list()
            for ts, _, kc_msg in kappac_msgs:
                kc_msg: Float64MultiArray

                if all(kc == 0 for kc in kc_msg.data): # try to skip all zero kappa c
                    continue

                viable_ts_msg.append((ts, kc_msg))

            # for

            best_ts = kappac_msgs[len(kappac_msgs)//2][0]
            if len(viable_ts_msg) > 0:
                best_ts = viable_ts_msg[len(viable_ts_msg)//2][0]

            else:
                warnings.warn(f"Insertion Depth {depth} mm has kappa_c = 0 for all messages.")

            timestamps[depth] = best_ts

        # for

        return timestamps
    
    # determine_timestamps

    def parse_data(self):
        assert self.timestamp_ranges is not None, (
            "You must configure the timestamp ranges for each of the insertion depths in order to "
            "parse the needle shape data"
        )

        self.timestamps = self.determine_timestamps()

        # topic names
        kappac_topic = list(filter(lambda topic: "state/kappac" in topic))[0]
        winit_topic  = list(filter(lambda topic: "state/winit" in topic))[0]
        shape_topic  = list(filter(lambda topic: "state/current_shape" in topic))[0]

        # parse the needle shape data
        for depth, ts_range in self.timestamp_ranges.items():
            target_ts = self.timestamps[depth]

            closest_kc_ts, best_kc_msg = self.get_closest_message_to_timestamp(
                kappac_topic,
                target_ts,
                ts_range=ts_range,
            )
            best_kc = None
            if best_kc_msg is not None:
                best_kc_msg: Float64MultiArray
                best_kc = np.asarray(best_kc_msg.data)

            # if

            self.needle_data[depth]["data"]["kappa_c"]      = best_kc
            self.needle_data[depth]["timestamp"]["kappa_c"] = closest_kc_ts

            # parse the needle's winit
            closest_winit_ts, best_winit_msg = self.get_closest_message_to_timestamp(
                winit_topic,
                target_ts,
                ts_range=ts_range
            )
            best_winit = None
            if best_winit_msg is not None:
                best_winit_msg: Float64MultiArray
                best_winit = np.asarray(best_winit_msg.data)

            # if

            self.needle_data[depth]["data"]["winit"]       = best_winit
            self.needle_data[depth]["timestamps"]["winit"] = closest_winit_ts

            # parse the needle's shape
            closest_shape_ts, best_shape_msg = self.get_closest_message_to_timestamp(
                shape_topic,
                target_ts,
                ts_range=ts_range
            )
            best_shape = None
            if best_shape_msg is not None:
                best_shape_msg: PoseArray
                best_shape = msg2poses(best_shape)[:, :3, 3] # take only the positions

            # if

            self.needle_data[depth]["data"]["shape"]       = best_shape
            self.needle_data[depth]["timestamps"]["shape"] = closest_shape_ts

        # for

        return self.needle_data
    
    # parse_data

    def save_results(self, odir: str, filename: str = None):
        assert len(self.needle_data) > 0, (
            "Needle data needs to be parsed first!"
        )
        
        filename = filename if filename is not None else "needle_data.xlsx"
        
        for depth, data_ts_dict in self.needle_data.items():
            sub_odir = os.path.join(odir, str(depth))
            os.makedirs(sub_odir, exist_ok=True)

            with pd.ExcelWriter(os.path.join(sub_odir, filename)) as xl_writer:
                # write the timestamp information (timestamps grabbed)
                ts_df = pd.DataFrame.from_dict(data_ts_dict["timestamps"], orient='index')
                ts_df.loc["target"]    = self.timestamps[depth]
                ts_df.loc["range_min"] = self.timestamp_ranges[depth][0]
                ts_df.loc["range_max"] = self.timestamp_ranges[depth][1]

                ts_df.to_excel(
                    xl_writer,
                    sheet_name="ROS timestamps",
                    index=True,
                )

                # write the data values
                for data_name, data in data_ts_dict["data"].items():
                    df = pd.DataFrame(data)
                    df.to_excel(
                        xl_writer,
                        sheet_name=data_name,
                        index=False,
                    )
                 
                # for
            # with

            print(
                f"Saved needle shape data for insertion depth {depth} mm to:",
                os.path.join(sub_odir, filename)    
            )

        # for

    # save_results

# class: NeedleDataBagParser

class InsertionExperimentBagParser( BagFileParser ):
    DEFAULT_TOPICS_OF_INTEREST = {
        "camera": None,
        "robot": RobotDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
        "needle": NeedleDataBagParser.DEFAULT_TOPICS_OF_INTEREST,
    }

    def __init__( self, bagdir: str, bagfile: str = None, yamlfile: str = None, topics: list = None ):
        super().__init__( bagdir, bagfile=bagfile, yamlfile=yamlfile )

        self.topics_of_interest = (
            topics 
            if topics is not None else 
            InsertionExperimentBagParser.DEFAULT_TOPICS_OF_INTEREST
        )

        # data containers


    # __init__
    
# class: InsertionExperimentBag