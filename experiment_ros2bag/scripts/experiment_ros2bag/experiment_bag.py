import itertools

from .bag_file_parser import BagFileParser
import pandas as pd
import numpy as np

from needle_shape_publisher.utilites import (
    msg2pose,
    msg2poses,
)


class InsertionExperimentBag( BagFileParser ):
    DEFAULT_TOPICS_OF_INTEREST = {
        "camera": [ "/".join([ns, topic, cam]) for ns, topic, cam in 
                      itertools.product(["/camera"],
                                         ["left", "right"], 
                                         ["image_raw", "image_rect_color", "camera_info"])
        ],
        "robot": [ "/".join([ns, topic, axis]) for ns, topic, axis in 
                      itertools.product(["/stage/axis"], 
                                        ["command", "position", "state/moving", "state/on"], 
                                        ["linear_stage", "x", "y", "z"])
        ],
        "needle": [ "/needle/sensor/raw", "/needle/sensor/processed",
                    "/needle/state/current_shape", "/needle/state/kappac","/needle/state/winit", 
                    "/needle/state/skin_entry", "/stage/state/needle_pose" 
        ],
    }

    def __init__( self, bagdir: str, bagfile: str = None, yamlfile: str = None, topics: list = None ):
        super().__init__( bagdir, bagfile=bagfile, yamlfile=yamlfile )

        self.topics_of_interest = topics if topics is not None else InsertionExperimentBag.DEFAULT_TOPICS_OF_INTEREST

        # data containers
        self.camera_data = list()
        self.robot_data  = list()
        self.needle_data = list()

    # __init__

    def parse_data(self, camera: bool = False, robot: bool = False, needle: bool = False):
        """ Parse the data  """
        # parse camera data
        if camera: # TODO
            pass 

        # if: camera

        # parse robot data
        if robot: # TODO
            # get the position messages as a generator (to not overload RAM)
            robot_position_topics = list(filter(lambda t: "/position/" in t, self.topics_of_interest['robot']))
            bag_rows = self.get_messages(topic_name=robot_position_topics, generator_count=len(robot_position_topics))

            # iterate through the generator
            for i, rows in enumerate(bag_rows):
                # parse the set of rows
                try:
                    robot_positions = {topic.replace('/stage/axis/position/', '') : (ts, msg.data) for ts, topic, msg in rows}
                
                    # make sure this set has all 4 messages
                    if len(robot_positions) != len(robot_position_topics):
                        continue
                    print(  robot_positions['x'][0]     )
                    # append to robot data
                    tupl=( 
                        robot_positions['x'][0], 
                        robot_positions['x'][1],
                        robot_positions['y'][1],
                        robot_positions['z'][1],
                        robot_positions['linear_stage'][1]
                    )
                    robot_state = {
                        'x': {
                            'timestamp': robot_positions['x'][0], 
                            'position': robot_positions['x'][1],
                        },
                        'y': {
                            'timestamp': robot_positions['y'][0], 
                            'position': robot_positions['y'][1],
                        },
                        'z': {
                            'timestamp': robot_positions['z'][0], 
                            'position': robot_positions['z'][1],
                        },
                        'linear_stage': {
                            'timestamp': robot_positions['linear_stage'][0], 
                            'position': robot_positions['linear_stage'][1],
                        },
                    }
                    self.robot_data.append(
                        tupl
                    )
                    print(tupl)
                except:
                    print("[ROBOT DATA PROCESSING]: exception caught")
                finally:
                    df = pd.DataFrame(
                        np.array(self.robot_data),
                        columns=[
                            'timestamp x', 'position x',
                            'timestamp y', 'position y',
                            'timestamp z', 'position z',
                            'timestamp linear_stage', 'position linear_stage',
                        ]    
                    )
                    df.to_csv('axes_positions4.csv', index=True)
                    print("saved axes positions")
                    break
                
                # debugging stuff
                # print( f"[{timestamp}]:" )
                # for axis, val in sorted(robot_positions.items()):
                #     print(f"  {axis:15s}: {val[1]:10.6f} mm")
                # print(100*"=")

                # if i >= 6:
                #     break

            #  for
        # if: robot

        # parse needle data
        if needle: 
            # get the needle messages as a generator (to not overload RAM)
            needle_shape_topics = list(filter(lambda t: "/needle/state/current_shape" in t, self.topics_of_interest['needle']))
            bag_rows = self.get_messages(topic_name=needle_shape_topics, generator_count=len(needle_shape_topics))

            # iterate through the generator
            for i, rows in enumerate(bag_rows):
                # parse the set of rows
                needle_positions = {
                    topic.replace('/needle/state/current_shape/', '') : (ts, msg.poses) for ts, topic, msg in rows
                }
                
                try:        
                    shape = msg2poses(needle_positions["/needle/state/current_shape"][1])[:, :3, 3]

                    # make sure this set has all required messages
                    if len(needle_positions) != len(needle_shape_topics):
                        continue
                    
                    print(needle_positions["/needle/state/current_shape"][0])
                
                    # append to needle data
                    self.needle_data.append(
                        ( needle_positions["/needle/state/current_shape"][0], 
                          shape)
                    )

                except:
                    print(f"[NEEDLE DATA PROCESSING]: There was an error at timestamp: {needle_positions['/needle/state/current_shape'][0]}")

                finally:
                    df = pd.DataFrame(np.array(self.needle_data))
                    df.to_csv('needle_shapes4.csv', index=True)
                    print("saved needle shapes")
                    break

            print("end of parsing")
            
            # # debugging stuff
            # print( f"[{timestamp}]:" )
            # for axis, val in sorted(robot_positions.items()):
            #     print(f"  {axis:15s}: {val[1]:10.6f} mm")
            # print(100*"=")

            # if i >= 6:
            #     break
            


# class: InsertionExperimentBag
