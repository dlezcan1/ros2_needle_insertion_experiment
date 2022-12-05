import itertools

from .bag_file_parser import BagFileParser
import pandas as pd
import numpy as np


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
        self.camera_data = []
        self.robot_data  = []
        self.needle_data = []

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
                # timestamp = min([ts for ts, *_ in robot_positions.values()])
                

                    # make sure this set has all 4 messages
                    if len(robot_positions) != len(robot_position_topics):
                        continue
                    print(  robot_positions['x'][0]     )
                    # append to robot data
                    tupl=( robot_positions['x'][0], 
                          robot_positions['x'][1],
                          robot_positions['y'][1],
                          robot_positions['z'][1],
                          robot_positions['linear_stage'][1] )
                    self.robot_data.append(
                        tupl
                    )
                    print(tupl)
                except:
                    print("exception caught")
                    df = pd.DataFrame(np.array(self.robot_data))
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
        if needle: # TODO
            # get the needle messages as a generator (to not overload RAM)
            needle_shape_topics = list(filter(lambda t: "/needle/state/current_shape" in t, self.topics_of_interest['needle']))
            bag_rows = self.get_messages(topic_name=needle_shape_topics, generator_count=len(needle_shape_topics))

            # iterate through the generator
            for i, rows in enumerate(bag_rows):
                # parse the set of rows
                
                robot_positions = {
                    topic.replace('/needle/state/current_shape/', '') : (ts, msg.poses) for ts, topic, msg in rows
                }
                # timestamp = min([ts for ts, *_ in robot_positions.values()])

                # print( timestamp, robot_positions["/needle/state/current_shape"][1][0].position    )
                try:        
                    shape=[]
                    for p in robot_positions["/needle/state/current_shape"][1]:
                        shape.append([p.position.x, p.position.y, p.position.z])

                    

                    # make sure this set has all 4 messages
                    if len(robot_positions) != len(needle_shape_topics):
                        continue
                    print(robot_positions["/needle/state/current_shape"][0])
                    #append to robot data
                    self.needle_data.append(
                        ( robot_positions["/needle/state/current_shape"][0], 
                          shape)
                    )
                except:
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
