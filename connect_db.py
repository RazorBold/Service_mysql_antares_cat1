import pymysql
import requests
from datetime import datetime, timedelta
import struct
import time

def create_database_connection():
    # Konfigurasi Database
    db_config = {
        'db_host': '127.0.0.1',           # Database host local
        'db_port': 3306,                  # Port database
        'db_name': 'lansitec_cat1',       # Nama database
        'db_user': 'admin',             # Username database
        'db_password': 'Wow0w0!2025'      # Password database
    }

    try:
        # Membuat koneksi database langsung
        connection = pymysql.connect(
            host=db_config['db_host'],
            port=db_config['db_port'],
            user=db_config['db_user'],
            password=db_config['db_password'],
            database=db_config['db_name']
        )

        return connection

    except Exception as e:
        print(f"Error saat membuat koneksi: {str(e)}")
        return None

def get_device_data(connection, filter_type=None, value=None):
    """
    Fungsi untuk mengambil data device dengan berbagai filter
    filter_type: 'imei', 'serial_number', atau None untuk semua data
    value: nilai yang dicari
    """
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:  # Menggunakan DictCursor agar hasil dalam bentuk dictionary
            if filter_type == 'imei':
                query = "SELECT * FROM device WHERE imei = %s"
                cursor.execute(query, (value,))
            elif filter_type == 'serial_number':
                query = "SELECT * FROM device WHERE serial_number = %s"
                cursor.execute(query, (value,))
            else:
                query = "SELECT * FROM device"
                cursor.execute(query)
            
            results = cursor.fetchall()
            return results
    except Exception as e:
        print(f"Error saat mengambil data: {str(e)}")
        return None

def get_table_data(connection, table_name):
    """
    Fungsi untuk mengambil semua data dari tabel yang dipilih
    """
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            results = cursor.fetchall()
            return results
    except Exception as e:
        print(f"Error saat mengambil data: {str(e)}")
        return None

def format_antares_date(date_str):
    """
    Mengkonversi format tanggal Antares (YYYYMMDDTHHmmss) ke format yang lebih readable
    """
    try:
        # Parse tanggal dari format "20250211T144600" ke datetime object
        dt = datetime.strptime(date_str, "%Y%m%dT%H%M%S")
        # Format ke string yang lebih readable
        return dt.strftime("%d-%m-%Y %H:%M:%S")
    except Exception as e:
        print(f"Error memformat tanggal: {str(e)}")
        return date_str

def decode_hex_message(hex_message):
    """
    Mendecode pesan hex dari content Antares
    """
    parts = []
    
    # Check message type first
    if len(hex_message) >= 2:
        msg_type = hex_message[0]
        
        if msg_type == '1':  # Registration message
            return {
                'type': 'Registration message',
                'raw_data': hex_message
            }
        elif msg_type == '2':  # Heartbeat message
            # Define field lengths for Type 2
            field_lengths = [2,  # Type Bit Field
                           8,  # State Bit Field
                           2,  # VOL Bit Field
                           2,  # VOL Percent Bit Field
                           2,  # BLE Receiving Count
                           2,  # GNSS-on Count
                           4,  # Temperature
                           4,  # Movement Duration
                           4,  # Reserved
                           4,  # Reserved
                           4,  # Message ID
                           8   # Reserved
                           ]
            
            # Split message into parts
            current_pos = 0
            for length in field_lengths:
                if current_pos + length <= len(hex_message):
                    parts.append(hex_message[current_pos:current_pos + length])
                    current_pos += length
            
            # Decode parts
            result = {
                'type': 'Heartbeat message',
                'battery_voltage': float(int(parts[2], 16)) * 0.1 if len(parts) > 2 else None,
                'battery_percent': int(parts[3], 16) if len(parts) > 3 else None,
                'temperature': int(parts[6][-2:], 16) if len(parts) > 6 else None,
                'movement_duration': int(parts[7], 16) * 5 if len(parts) > 7 else None,
                'raw_data': hex_message
            }
            return result
            
        elif msg_type == '3':  # GNSS Position message
            # Define field lengths for Type 3
            field_lengths = [2,  # Type Bit Field
                           8,  # Longitude
                           8,  # Latitude
                           8,  # UTC Time
                           4,  # Reserved
                           4,  # Reserved
                           2]  # Reserved
            
            # Split message into parts
            current_pos = 0
            for length in field_lengths:
                if current_pos + length <= len(hex_message):
                    parts.append(hex_message[current_pos:current_pos + length])
                    current_pos += length
            
            # Decode IEEE 754 coordinates
            try:
                latitude = struct.unpack('!f', bytes.fromhex(parts[2]))[0] if len(parts) > 2 else None
                longitude = struct.unpack('!f', bytes.fromhex(parts[1]))[0] if len(parts) > 1 else None
                timestamp = int(parts[3], 16) if len(parts) > 3 else None
                utc_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S UTC') if timestamp else None
            except Exception as e:
                print(f"Error decoding coordinates: {str(e)}")
                latitude = longitude = utc_time = None
            
            result = {
                'type': 'GNSS Position message',
                'latitude': latitude,
                'longitude': longitude,
                'utc_time': utc_time,
                'raw_data': hex_message
            }
            return result
            
        elif msg_type == '4':  # Beacon message
            return {
                'type': 'Beacon message',
                'raw_data': hex_message
            }
            
        elif msg_type == '5':  # Alarm message
            # Define field lengths for Type 5
            field_lengths = [2,      # Type Bit Field
                           2,      # Alarm Bit Field
                           2]      # Reserved
            
            # Split message into parts
            current_pos = 0
            for length in field_lengths:
                if current_pos + length <= len(hex_message):
                    parts.append(hex_message[current_pos:current_pos + length])
                    current_pos += length
            
            # Decode alarm type
            alarm_types = {
                '01': 'Low battery alarm',
                '02': 'Power off alarm',
                '03': 'Power on alarm',
                '04': 'Movement alarm',
                '05': 'Tamper detection alarm'
            }
            
            alarm_code = parts[1] if len(parts) > 1 else None
            alarm_description = alarm_types.get(alarm_code, 'Unknown alarm type')
            
            result = {
                'type': 'Alarm message',
                'alarm_type': alarm_code,
                'alarm_description': alarm_description,
                'raw_data': hex_message
            }
            return result
            
        else:
            return {
                'type': 'Unknown message type',
                'raw_data': hex_message
            }
    
    return {
        'type': 'Invalid message',
        'raw_data': hex_message
    }

def save_to_registration(connection, parsed_data, imei, created_time):
    """
    Menyimpan data yang sudah di-parse ke tabel registration
    """
    try:
        with connection.cursor() as cursor:
            # Cek data duplikat
            check_query = """
            SELECT id FROM registration 
            WHERE payload_id_1 = %s AND timestamp = %s 
            LIMIT 1
            """
            cursor.execute(check_query, (imei, created_time))
            existing_data = cursor.fetchone()
            
            if existing_data is None:
                # Siapkan data dasar
                data = {
                    'payload_id_1': imei,
                    'payload_id_2': parsed_data['type'].split()[0],  # Mengambil tipe pesan (Heartbeat/GNSS/Alarm)
                    'parsed_data': str(parsed_data),
                    'timestamp': created_time,
                    'latitude': None,
                    'longitude': None,
                    'Major': None,
                    'Minor': None,
                    'voltage': None,
                    'persentase_baterai': None,
                    'alarm': None
                }
                
                # Update data sesuai tipe pesan
                if parsed_data['type'] == 'GNSS Position message':
                    data.update({
                        'latitude': parsed_data['latitude'],
                        'longitude': parsed_data['longitude']
                    })
                    print("\nMenyimpan data GNSS ke tabel registration:")
                    print(f"• IMEI: {imei}")
                    print(f"• Type: {data['payload_id_2']}")
                    print(f"• Latitude: {data['latitude']}")
                    print(f"• Longitude: {data['longitude']}")
                    print(f"• Timestamp: {data['timestamp']}")
                    
                elif parsed_data['type'] == 'Heartbeat message':
                    data.update({
                        'voltage': parsed_data['battery_voltage'],
                        'persentase_baterai': parsed_data['battery_percent']
                    })
                    print("\nMenyimpan data Heartbeat ke tabel registration:")
                    print(f"• IMEI: {imei}")
                    print(f"• Type: {data['payload_id_2']}")
                    print(f"• Voltage: {data['voltage']}V")
                    print(f"• Battery: {data['persentase_baterai']}%")
                    print(f"• Timestamp: {data['timestamp']}")
                    
                elif parsed_data['type'] == 'Alarm message':
                    data.update({
                        'alarm': 'ON'
                    })
                    print("\nMenyimpan data Alarm ke tabel registration:")
                    print(f"• IMEI: {imei}")
                    print(f"• Type: {data['payload_id_2']}")
                    print(f"• Alarm Status: ON")
                    print(f"• Alarm Type: {parsed_data['alarm_description']}")
                    print(f"• Timestamp: {data['timestamp']}")

                # Buat query insert
                fields = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
                insert_query = f"""
                INSERT INTO registration ({fields})
                VALUES ({placeholders})
                """
                
                cursor.execute(insert_query, list(data.values()))
                connection.commit()
                print("Data berhasil disimpan ke tabel registration")
            else:
                print(f"Data untuk IMEI {imei} dengan timestamp {created_time} sudah ada di tabel registration")
            
    except Exception as e:
        print(f"Error saat menyimpan ke tabel registration: {str(e)}")

def save_payload_to_db(connection, imei, content, created_time):
    """
    Menyimpan data payload ke database dengan hasil parsing
    """
    try:
        # Parse content terlebih dahulu
        parsed_data = decode_hex_message(content)
        
        # Tampilkan informasi parsing secara terstruktur
        print("\nHasil Parsing Data:")
        print("=" * 50)
        print(f"IMEI Device: {imei}")
        print(f"Tipe Pesan: {parsed_data['type']}")
        print(f"Waktu: {created_time}")
        print("-" * 50)
        print("Detail Data:")
        
        # Tampilkan detail sesuai tipe pesan
        if parsed_data['type'] == 'Heartbeat message':
            print(f"• Battery Voltage  : {parsed_data['battery_voltage']}V")
            print(f"• Battery Level   : {parsed_data['battery_percent']}%")
            print(f"• Temperature     : {parsed_data['temperature']}°C")
            print(f"• Movement Duration: {parsed_data['movement_duration']} seconds")
        
        elif parsed_data['type'] == 'GNSS Position message':
            print(f"• Latitude  : {parsed_data['latitude']}")
            print(f"• Longitude : {parsed_data['longitude']}")
            print(f"• UTC Time  : {parsed_data['utc_time']}")
        
        elif parsed_data['type'] in ['Beacon message', 'Alarm message']:
            print(f"• Raw Data: {parsed_data['raw_data']}")
        
        print("-" * 50)
        print(f"Raw Hex Data: {content}")
        print("=" * 50)
        
        # Simpan ke tabel payload
        with connection.cursor() as cursor:
            # Insert ke tabel payload
            insert_query = """
            INSERT INTO payload (device_id, payload, parsed_payload, created_at) 
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (imei, content, str(parsed_data), created_time))
            connection.commit()
            
            # Simpan ke tabel registration menggunakan IMEI dan created_time
            save_to_registration(connection, parsed_data, imei, created_time)
            
            print("Status: Data berhasil disimpan ke database")
                
    except Exception as e:
        print(f"Error saat menyimpan payload: {str(e)}")

def get_last_timestamp(connection, imei):
    """
    Mengambil timestamp terakhir dari database untuk IMEI tertentu
    """
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT timestamp 
            FROM registration 
            WHERE payload_id_1 = %s 
            ORDER BY timestamp DESC 
            LIMIT 1
            """
            cursor.execute(query, (imei,))
            result = cursor.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"Error saat mengambil timestamp terakhir: {str(e)}")
        return None

def get_antares_data(connection):
    """
    Fungsi untuk mengambil IMEI dan data dari Antares
    """
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            query = "SELECT imei FROM device"
            cursor.execute(query)
            results = cursor.fetchall()
            
            base_url = "https://platform.antares.id:8443/~/antares-cse/antares-id/Tracker_Tanto/Lansitec_CAT1_{}/la"
            headers = {
                'X-M2M-Origin': '5aaf96c4cba802b1:e5c2804b39d4bad8',
                'Content-Type': 'application/json;ty=4',
                'Accept': 'application/json'
            }
            
            for device in results:
                imei = device['imei']
                
                # Validasi format IMEI
                if not imei.isdigit() or len(imei) < 8:
                    print(f"\nIMEI tidak valid: {imei}")
                    continue

                # Cek timestamp terakhir
                last_timestamp = get_last_timestamp(connection, imei)
                
                try:
                    request_url = base_url.format(imei)
                    print(f"\nMengakses URL: {request_url}")
                    
                    response = requests.get(request_url, headers=headers, verify=False)
                    
                    if response.status_code == 200:
                        data = response.json()
                        content = data['m2m:cin']['con']
                        created_time = data['m2m:cin']['ct']
                        
                        # Convert Antares time format ke MySQL datetime
                        dt = datetime.strptime(created_time, "%Y%m%dT%H%M%S")
                        mysql_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")
                        
                        if last_timestamp is None or mysql_datetime > last_timestamp.strftime("%Y-%m-%d %H:%M:%S"):
                            save_payload_to_db(connection, imei, content, mysql_datetime)
                        else:
                            print(f"IMEI {imei}: Data sudah ada (timestamp: {mysql_datetime})")
                    
                    elif response.status_code == 404:
                        print(f"\nIMEI: {imei}")
                        print("Error 404: Container tidak ditemukan di Antares")
                        print("Kemungkinan penyebab:")
                        print("1. IMEI belum terdaftar di Antares")
                        print("2. Container belum dibuat untuk IMEI tersebut")
                        print("3. Nama container tidak sesuai format yang diharapkan")
                    
                    else:
                        print(f"\nIMEI: {imei}")
                        print(f"Error mengambil data: HTTP {response.status_code}")
                        print(f"Response: {response.text[:200]}")  # Tampilkan 200 karakter pertama dari response
                        
                except requests.exceptions.RequestException as e:
                    print(f"\nIMEI: {imei}")
                    print(f"Network error saat request Antares: {str(e)}")
                except Exception as e:
                    print(f"\nIMEI: {imei}")
                    print(f"Error umum saat request Antares: {str(e)}")
                
                print("-" * 50)
            
            return results
            
    except Exception as e:
        print(f"Error saat mengambil data dari database: {str(e)}")
        return None

def create_or_update_payload_table(connection):
    """
    Membuat atau mengupdate struktur tabel payload
    """
    try:
        with connection.cursor() as cursor:
            # Cek apakah kolom parsed_payload sudah ada
            check_column = """
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'payload' 
            AND COLUMN_NAME = 'parsed_payload'
            """
            cursor.execute(check_column)
            column_exists = cursor.fetchone()
            
            if not column_exists:
                # Tambah kolom parsed_payload jika belum ada
                alter_table = """
                ALTER TABLE payload 
                ADD COLUMN parsed_payload TEXT
                """
                cursor.execute(alter_table)
                connection.commit()
                print("Kolom parsed_payload berhasil ditambahkan ke tabel payload")
                
            # Cek kolom created_at
            check_column = """
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'payload' 
            AND COLUMN_NAME = 'created_at'
            """
            cursor.execute(check_column)
            column_exists = cursor.fetchone()
            
            if not column_exists:
                # Tambah kolom created_at jika belum ada
                alter_table = """
                ALTER TABLE payload 
                ADD COLUMN created_at DATETIME
                """
                cursor.execute(alter_table)
                connection.commit()
                print("Kolom created_at berhasil ditambahkan ke tabel payload")
    except Exception as e:
        print(f"Error saat mengupdate struktur tabel: {str(e)}")

def main():
    print("Starting data collection service...")
    
    while True:
        try:
            # Membuat koneksi langsung ke database
            connection = create_database_connection()
            
            if connection:
                try:
                    # Update struktur tabel
                    create_or_update_payload_table(connection)
                    
                    # Mengambil data dari Antares
                    print("\n" + "="*50)
                    print(f"Fetching data at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print("="*50)
                    
                    devices = get_antares_data(connection)
                    if not devices:
                        print("Tidak ada data device yang ditemukan")

                except Exception as e:
                    print(f"Error saat menjalankan query: {str(e)}")

                finally:
                    # Menutup koneksi
                    connection.close()
                    print("\nKoneksi ditutup")
            
            # Tunggu 10 detik sebelum mengecek data baru
            print(f"\nWaiting for 10 seconds before next check...")
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\nProgram dihentikan oleh user")
            break
        except Exception as e:
            print(f"\nError dalam main loop: {str(e)}")
            print("Mencoba kembali dalam 10 detik...")
            time.sleep(10)

if __name__ == "__main__":
    main()
