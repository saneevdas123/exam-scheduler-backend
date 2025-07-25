import pandas as pd
import networkx as nx
from flask import Flask, jsonify, request
import os
import logging

# Suppress Flask development server messages
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# Initialize Flask app
app = Flask(__name__)

# Global variables to store data and timetable
student_subjects_map = {}
conflict_graph = None
generated_timetable = {}

# --- Configuration ---
# Ensure this path is correct on your Ubuntu server
# You will need to upload your CSV file to this location on the server.
CSV_FILE_PATH = '/path/to/your/Subject Registration Data- ODD SEM 2024-25 - 3rd,5th&7th-Final (1).xlsx - Sheet1 (2).csv'
SLOTS_PER_DAY = 2 # You can change this to 3 or more if needed

# --- Helper Functions ---

def load_data(file_path):
    """
    Loads data from the CSV file and creates the student-subject mapping.
    """
    global student_subjects_map
    try:
        df = pd.read_csv(file_path)

        # Drop rows with any missing values in relevant columns
        df.dropna(subset=['Rollno', 'Course Name'], inplace=True)

        # Convert Rollno to string to ensure consistent key type
        df['Rollno'] = df['Rollno'].astype(str)

        # Create student-subject mapping
        student_subjects_map = (
            df.groupby('Rollno')['Course Name']
            .apply(list)
            .to_dict()
        )
        print(f"Data loaded successfully. Found {len(student_subjects_map)} students.")
        return True
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return False
    except Exception as e:
        print(f"Error loading data: {e}")
        return False

def build_conflict_graph():
    """
    Builds the conflict graph where nodes are subjects and edges represent conflicts.
    A conflict exists if at least one student is registered for both subjects.
    """
    global conflict_graph
    if not student_subjects_map:
        print("Error: Student-subject map is empty. Load data first.")
        return False

    conflict_graph = nx.Graph()
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

        generated_timetable = {}
        for subject, slot_index in coloring.items():
            day_num = (slot_index // SLOTS_PER_DAY) + 1
            slot_in_day = (slot_index % SLOTS_PER_DAY) + 1
            slot_name = f"Day-{day_num} Slot-{slot_in_day}"
            generated_timetable[subject] = slot_name

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
    """
    global generated_timetable

    # Check if the CSV file exists. If not, inform the user to upload it.
    if not os.path.exists(CSV_FILE_PATH):
        return jsonify({
            "status": "error",
            "message": f"CSV file '{CSV_FILE_PATH}' not found on the server. Please ensure it's uploaded to the specified path."
        }), 404

    # Load data
    if not load_data(CSV_FILE_PATH):
        return jsonify({
            "status": "error",
            "message": "Failed to load data. Check server logs for details."
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

    return jsonify({
        "status": "success",
        "message": "Timetable generated successfully.",
        "timetable": generated_timetable,
        "notes": [
            "This timetable uses abstract 'Day-X Slot-Y' assignments.",
            "The number of slots per day is configured as " + str(SLOTS_PER_DAY) + ".",
            "No student will have two exams in the same slot.",
            "All campuses are assumed to have the exam for a given subject on the same day and slot."
        ]
    })

# The __name__ == '__main__' block is removed for Gunicorn deployment
# Gunicorn will import the 'app' object directly from this file.
