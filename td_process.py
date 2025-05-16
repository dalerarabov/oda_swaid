# me - this DAT
# frame - the current frame
# state - True if the timeline is paused
# Make sure the corresponding toggle is enabled in the Execute DAT.

import json
import re
from datetime import datetime
from typing import Dict, List, Optional

# region [Constants]
PARAMS = ['hr', 'lf_hf_ratio', 'rmssd', 'sdrr', 'si']
MAC_PATTERN = re.compile(r'^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$', re.IGNORECASE)
# endregion

# region [Type Definitions]
class Device:
    def __init__(self, mac: str, name: str, process: bool):
        self.mac = mac.upper()
        self.name = name
        self.process = process

class Measurement:
    def __init__(self, device_mac: str, timestamp: str, values: Dict[str, float]):
        self.device_mac = device_mac.upper()
        self.timestamp = timestamp
        self.values = values
# endregion

# region [State]
_devices_cache = None
_last_devices_text = None
# endregion

def validate_mac(mac: str) -> str:
    """Normalize and validate MAC address format."""
    mac = mac.upper().replace('-', ':')
    if not MAC_PATTERN.match(mac):
        raise ValueError(f"Invalid MAC address format: {mac}")
    return mac

def parse_devices(dvs_text: str) -> List[Device]:
    """Parse and cache devices with validation."""
    global _devices_cache, _last_devices_text
    
    if dvs_text == _last_devices_text:
        return _devices_cache
    
    devices = []
    for item in json.loads(dvs_text):
        try:
            mac = validate_mac(item['mac_address'])
            devices.append(Device(
                mac=mac,
                name=item['name'],
                process=item.get('process', False)
            ))
        except (KeyError, ValueError) as e:
            print(f"Device validation error: {e}")
    
    _devices_cache = devices
    _last_devices_text = dvs_text
    return devices

def parse_measurements(data_text: str) -> List[Measurement]:
    """Parse measurements with error handling."""
    measurements = []
    for item in json.loads(data_text):
        try:
            measurements.append(Measurement(
                device_mac=validate_mac(item['device_mac']),
                timestamp=item['timestamp'],
                values={p: float(item.get(p, 0)) for p in PARAMS}
            ))
        except (KeyError, ValueError) as e:
            print(f"Measurement error: {e}")
    return measurements

def update_output_table(devices: List[Device], measurements: List[Measurement], output_dat):
    """Efficient table update with batched operations."""
    # Prepare header
    header = ['Channel'] + [str(i) for i in range(60)]
    
    # Prepare data matrix
    device_map = {d.mac: d for d in devices if d.process}
    channel_data = {}
    
    # Process measurements
    for m in measurements:
        device = device_map.get(m.device_mac)
        if not device:
            continue
        
        try:
            sec = datetime.strptime(m.timestamp, "%Y-%m-%d %H:%M:%S").second
            if sec >= 60:
                continue
        except ValueError:
            continue
        
        for param in PARAMS:
            channel = f"{device.name}_{param}"
            if channel not in channel_data:
                channel_data[channel] = [0.0]*60
            channel_data[channel][sec] = m.values.get(param, 0.0)
    
    # Prepare rows
    rows = [header]
    for channel, values in sorted(channel_data.items()):
        rows.append([channel] + [f"{v:.1f}" for v in values])
    
    # Batch update
    output_dat.clear()
    output_dat.appendRows(rows)

def process_data():
    """Main processing pipeline with error handling."""
    try:
        dvs_op = op('dvs_json')
        data_op = op('data_json')
        output_op = op('output_table')
        
        if not all([dvs_op, data_op, output_op]):
            raise ValueError("Missing required operators")
        
        devices = parse_devices(dvs_op.text)
        measurements = parse_measurements(data_op.text)
        update_output_table(devices, measurements, output_op)
        
    except Exception as e:
        print(f"Processing error: {repr(e)}")
        debug(e)

def onStart():
    pass

def onCreate():
    pass

def onExit():
    pass

def onFrameStart(frame):
    process_data()

def onFrameEnd(frame):
    pass

def onPlayStateChange(state):
    pass

def onDeviceChange():
    pass

def onProjectPreSave():
    pass

def onProjectPostSave():
    pass