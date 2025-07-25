import pandas as pd
import networkx as nx
from flask import Flask, jsonify, request, Response # Import Response for CSV output
import os
import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import io
import uuid # For generating unique file keys

# Suppress Flask development server messages
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# Initialize Flask app
app = Flask(__name__)

# Global variables to store data and timetable
# These will be reset per request for dynamic data loading
student_subjects_map = {}
conflict_graph = None
generated_timetable = {}

# --- Configuration ---
# AWS credentials should be set as environment variables on your server
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1') # Default region, or set as env var

# IMPORTANT: Configure the S3 bucket where files will be uploaded
UPLOAD_S3_BUCKET_NAME = 'your-upload-s3-bucket' # <<< CHANGE THIS to your S3 bucket name for uploads

SLOTS_PER_DAY = 2 # You can change this to 3 or more if needed

# --- Helper Functions ---

def load_data_from_s3(bucket_name, file_key, aws_access_key_id, aws_secret_access_key, aws_region_name):
    """
    Loads data from an S3 bucket and creates the student-subject mapping.
    """
    global student_subjects_map
    # Clear previous data for a new request
    student_subjects_map = {}
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name
        )
        # Check if the file exists before attempting to get it
        s3.head_object(Bucket=bucket_name, Key=file_key)
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        # Determine file type and read accordingly
        if file_key.lower().endswith('.csv'):
            csv_content = obj['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_content))
        elif file_key.lower().endswith('.xlsx'):
            excel_content = io.BytesIO(obj['Body'].read())
            df = pd.read_excel(excel_content)
        else:
            print(f"Error: Unsupported file format for S3 key: {file_key}. Only .csv and .xlsx are supported.")
            return False

        # Drop rows with any missing values in relevant columns
        df.dropna(subset=['Rollno', 'Name', 'Course Name'], inplace=True) # Added 'Name' for future use

        # Convert Rollno to string to ensure consistent key type
        df['Rollno'] = df['Rollno'].astype(str)

        # Create student-subject mapping
        # Store full student details for later use if needed
        temp_student_subjects = {}
        for _, row in df.iterrows():
            rollno = row['Rollno']
            name = row['Name'] # Assuming 'Name' column exists
            course_name = row['Course Name']
            if rollno not in temp_student_subjects:
                temp_student_subjects[rollno] = {'name': name, 'subjects': []}
            temp_student_subjects[rollno]['subjects'].append(course_name)

        # Reformat for the conflict graph, keeping track of students per subject
        # This will allow us to include student names/roll numbers in the final output
        # if the frontend requests it.
        student_subjects_map = {rollno: data['subjects'] for rollno, data in temp_student_subjects.items()}
        
        # Also store a map of subject to students for detailed output
        # This is a new addition to support the request for student names/roll numbers
        app.config['subject_to_students_map'] = {}
        for _, row in df.iterrows():
            subject = row['Course Name']
            rollno = row['Rollno']
            name = row['Name']
            if subject not in app.config['subject_to_students_map']:
                app.config['subject_to_students_map'][subject] = []
            # Avoid duplicate student entries for the same subject
            if {'rollno': rollno, 'name': name} not in app.config['subject_to_students_map'][subject]:
                app.config['subject_to_students_map'][subject].append({'rollno': rollno, 'name': name})


        print(f"Data loaded successfully from S3. Found {len(student_subjects_map)} students.")
        return True
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please configure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            print(f"Error: S3 object '{file_key}' not found in bucket '{bucket_name}'. Check file key and bucket name.")
        elif error_code == 'AccessDenied':
            print(f"Error: Access denied to S3 bucket '{bucket_name}' or object '{file_key}'. Check IAM permissions and bucket policy.")
        else:
            print(f"Error accessing S3: {e}")
        return False
    except Exception as e:
        print(f"Error loading data from S3: {e}")
        return False

def build_conflict_graph():
    """
    Builds the conflict graph where nodes are subjects and edges represent conflicts.
    A conflict exists if at least one student is registered for both subjects.
    """
    global conflict_graph
    # Clear previous graph for a new request
    conflict_graph = nx.Graph()

    if not student_subjects_map:
        print("Error: Student-subject map is empty. Load data first.")
        return False

    all_subjects = set()

    # Collect all unique subjects first
    for subjects in student_subjects_map.values():
        all_subjects.update(subjects)

    # Add all unique subjects as nodes to the graph
    for subject in all_subjects:
        conflict_graph.add_node(subject)

    # Identify conflicts: if a student takes two subjects, add an edge between them
    for student_id, subjects in student_subjects_map.items():
        # Add edges between every pair of subjects taken by the same student
        for i in range(len(subjects)):
            for j in range(i + 1, len(subjects)):
                subject1 = subjects[i]
                subject2 = subjects[j]
                if subject1 != subject2: # Ensure not adding self-loops if data has duplicates
                    conflict_graph.add_edge(subject1, subject2)

    print(f"Conflict graph built with {conflict_graph.number_of_nodes()} subjects and {conflict_graph.number_of_edges()} conflicts.")
    return True

def generate_timetable_slots():
    """
    Generates the examination timetable using graph coloring.
    Assigns subjects to abstract slots (day-X slot-Y).
    """
    global generated_timetable
    # Clear previous timetable for a new request
    generated_timetable = {}

    if conflict_graph is None or conflict_graph.number_of_nodes() == 0:
        print("Error: Conflict graph not built or is empty.")
        return False

    try:
        # Use greedy_color to assign a color (slot number) to each subject
        # This is a heuristic and doesn't guarantee the minimum number of slots,
        # but it's efficient and generally effective.
        coloring = nx.coloring.greedy_color(conflict_graph, strategy='largest_first')

        # Find the maximum slot number assigned
        max_slot_index = max(coloring.values()) if coloring else -1
        print(f"Graph coloring completed. Max slot index used: {max_slot_index}")

        # Construct the timetable including student details
        # This structure matches the one expected by the 'generate-timetable-file' script
        for subject, slot_index in coloring.items():
            day_num = (slot_index // SLOTS_PER_DAY) + 1
            slot_in_day = (slot_index % SLOTS_PER_DAY) + 1
            slot_name = f"Day-{day_num} Slot-{slot_in_day}"
            
            students_for_subject = app.config.get('subject_to_students_map', {}).get(subject, [])
            
            generated_timetable[subject] = {
                "slot": slot_name,
                "students": students_for_subject
            }

        print("Timetable generated successfully.")
        return True
    except Exception as e:
        print(f"Error generating timetable: {e}")
        return False

# --- Flask API Endpoints ---

@app.route('/')
def home():
    """
    Home endpoint for the API.
    """
    return "Welcome to the Examination Timetable API! Use /generate_timetable to get the timetable."

@app.route('/generate_timetable', methods=['GET'])
def get_timetable():
    """
    API endpoint to generate and return the examination timetable.
    Accepts 'bucket_name' and 'file_key' as query parameters.
    """
    bucket_name = request.args.get('bucket_name')
    file_key = request.args.get('file_key')

    if not bucket_name or not file_key:
        return jsonify({
            "status": "error",
            "message": "Missing 'bucket_name' or 'file_key' query parameters."
        }), 400

    # Load data from S3 using dynamic parameters
    if not load_data_from_s3(bucket_name, file_key, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME):
        return jsonify({
            "status": "error",
            "message": "Failed to load data from S3. Check server logs, S3 configuration, and provided bucket/key."
        }), 500

    # Build conflict graph
    if not build_conflict_graph():
        return jsonify({
            "status": "error",
            "message": "Failed to build conflict graph. Check server logs for details."
        }), 500

    # Generate timetable
    if not generate_timetable_slots():
        return jsonify({
            "status": "error",
            "message": "Failed to generate timetable. Check server logs for details."
        }), 500

    # Convert the generated_timetable dictionary to a list of objects for the frontend
    # This matches the structure expected by the 'generate-timetable-file' script
    formatted_timetable_output = []
    for subject, details in generated_timetable.items():
        formatted_timetable_output.append({
            "subject": subject,
            "slot": details["slot"],
            "students": details["students"]
        })

    return jsonify({
        "status": "success",
        "message": "Timetable generated successfully.",
        "timetable": formatted_timetable_output, # Return as a list of objects
        "notes": [
            "This timetable uses abstract 'Day-X Slot-Y' assignments.",
            "The number of slots per day is configured as " + str(SLOTS_PER_DAY) + ".",
            "No student will have two exams in the same slot.",
            "All campuses are assumed to have the exam for a given subject on the same day and slot."
        ]
    })

@app.route('/convert_xlsx_to_csv', methods=['POST'])
def convert_xlsx_to_csv():
    """
    API endpoint to convert an uploaded XLSX file to CSV.
    Expects the XLSX file in the 'file' field of a multipart/form-data request.
    Returns the CSV content directly.
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part in the request"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400

    if file and file.filename.lower().endswith('.xlsx'):
        try:
            # Read the XLSX file directly from the incoming stream
            excel_content = io.BytesIO(file.read())
            df = pd.read_excel(excel_content)

            # Convert DataFrame to CSV string
            csv_output = io.StringIO()
            df.to_csv(csv_output, index=False)
            csv_output.seek(0) # Rewind to the beginning

            # Return the CSV content as a response
            return Response(
                csv_output.getvalue(),
                mimetype='text/csv',
                headers={"Content-disposition": "attachment; filename=converted_data.csv"}
            )
        except Exception as e:
            print(f"Error during XLSX to CSV conversion: {e}")
            return jsonify({"status": "error", "message": f"Error processing file: {e}"}), 500
    else:
        return jsonify({"status": "error", "message": "Invalid file type. Only .xlsx files are supported."}), 400

@app.route('/upload_xlsx_to_s3', methods=['POST'])
def upload_xlsx_to_s3():
    """
    API endpoint to upload an XLSX file from the frontend directly to S3.
    Expects the XLSX file in the 'file' field of a multipart/form-data request.
    Returns the S3 bucket and file key upon successful upload.
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part in the request"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400

    if not file.filename.lower().endswith('.xlsx'):
        return jsonify({"status": "error", "message": "Invalid file type. Only .xlsx files are supported for upload."}), 400

    if not UPLOAD_S3_BUCKET_NAME or UPLOAD_S3_BUCKET_NAME == 'your-upload-s3-bucket':
        return jsonify({"status": "error", "message": "S3 upload bucket not configured on the server."}), 500

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION_NAME
        )

        # Generate a unique file name to avoid overwrites
        original_filename = file.filename
        file_extension = os.path.splitext(original_filename)[1]
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        s3_file_key = f"uploads/{unique_filename}" # You can customize the S3 path

        # Upload the file directly from the stream
        s3.upload_fileobj(file, UPLOAD_S3_BUCKET_NAME, s3_file_key)

        print(f"File '{original_filename}' uploaded to s3://{UPLOAD_S3_BUCKET_NAME}/{s3_file_key}")

        return jsonify({
            "status": "success",
            "message": "File uploaded to S3 successfully.",
            "bucket_name": UPLOAD_S3_BUCKET_NAME,
            "file_key": s3_file_key
        }), 200

    except NoCredentialsError:
        print("Error: AWS credentials not found for S3 upload.")
        return jsonify({"status": "error", "message": "AWS credentials not configured on the server."}), 500
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"S3 ClientError during upload: {error_code} - {e}")
        if error_code == 'AccessDenied':
            return jsonify({"status": "error", "message": "S3 access denied. Check IAM permissions for bucket."}), 500
        else:
            return jsonify({"status": "error", "message": f"S3 upload failed: {e}"}), 500
    except Exception as e:
        print(f"An unexpected error occurred during S3 upload: {e}")
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500

# The __name__ == '__main__' block is removed for Gunicorn deployment
# Gunicorn will import the 'app' object directly from this file.
