from db_manager import DBManager
import json


def import_surf_data_from_json(db_manager, json_file_path):
    with open(json_file_path, 'r') as json_file:
        surf_data = json.load(json_file)

        for spot_data in surf_data:
            spot_id = spot_data.get("spot_id")
            spot_name = spot_data.get("spot_name")

            if spot_id and spot_name:
                # Insert surf spot
                surf_spot = (spot_id, spot_name)
                db_manager.insert_surf_spot(surf_spot)
                print(f"Inserted surf spot: {spot_name}")

                # Insert surf cameras if available
                cameras = spot_data.get("cameras", [])
                for camera in cameras:
                    cam_number = camera.get("cam_number")
                    cam_name = camera.get("cam_name")
                    rewind_link_extension = camera.get("rewind_link_extension")

                    if cam_number and cam_name and rewind_link_extension:
                        surf_cam = (spot_id, cam_number, cam_name, rewind_link_extension)
                        db_manager.insert_surf_cam(surf_cam)
                        print(f"Inserted surf cam: {cam_name}")


db_manager = DBManager('SurfForecastDB')
json_data_file_path = 'surf_spots_data.json'
import_surf_data_from_json(db_manager, json_data_file_path)

db_manager.close_connection()
