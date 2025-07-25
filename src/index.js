
// --- Imports ---
const express = require('express');
const multer = require('multer'); // For handling multipart/form-data (file uploads)
const { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const XLSX = require('xlsx'); // For reading XLSX files
const csv = require('csv-parser'); // For parsing CSV streams
const { Readable } = require('stream'); // Node.js stream utility
const { v4: uuidv4 } = require('uuid'); // For generating unique IDs
const cors = require('cors'); // For Cross-Origin Resource Sharing

// --- Initialize Express App ---
const app = express();
const port = 5000; // Port for the Node.js application

// --- Middleware ---
app.use(express.json()); // For parsing application/json
app.use(express.urlencoded({ extended: true })); // For parsing application/x-www-form-urlencoded
app.use(cors()); // Enable CORS for all origins. In production, configure this more strictly.

// Multer setup for in-memory file storage
const upload = multer({ storage: multer.memoryStorage() });

// --- Configuration ---
// AWS credentials should be set as environment variables on your server for security.
// DO NOT hardcode them in production.
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const AWS_REGION_NAME = process.env.AWS_DEFAULT_REGION || 'us-east-1'; // Default region if not set

// IMPORTANT: Configure the S3 bucket where files will be uploaded
const UPLOAD_S3_BUCKET_NAME = process.env.UPLOAD_S3_BUCKET_NAME || 'your-upload-s3-bucket'; // <<< CHANGE THIS in your environment

const SLOTS_PER_DAY = 2; // You can change this to 3 or more if needed

// Initialize S3 Client
const s3Client = new S3Client({
    region: AWS_REGION_NAME,
    credentials: {
        accessKeyId: AWS_ACCESS_KEY_ID,
        secretAccessKey: AWS_SECRET_ACCESS_KEY,
    },
});

// --- Global Variables (reset per request in functions) ---
let studentSubjectsMap = new Map(); // Map<Rollno, Array<Subject>>
let subjectToStudentsMap = new Map(); // Map<Subject, Array<{rollno, name}>>

// --- Helper Functions ---

/**
 * Loads data from an S3 bucket, parses it (XLSX or CSV), and populates
 * studentSubjectsMap and subjectToStudentsMap.
 * @param {string} bucketName The S3 bucket name.
 * @param {string} fileKey The S3 object key (path to file).
 * @returns {Promise<boolean>} True if data loaded successfully, false otherwise.
 */
async function loadDataFromS3(bucketName, fileKey) {
    studentSubjectsMap = new Map(); // Reset for new request
    subjectToStudentsMap = new Map(); // Reset for new request

    try {
        // Verify file existence and access
        await s3Client.send(new HeadObjectCommand({ Bucket: bucketName, Key: fileKey }));

        const { Body } = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: fileKey }));

        if (!Body) {
            console.error(`S3 object body is empty for ${fileKey}`);
            return false;
        }

        const chunks = [];
        for await (const chunk of Body) {
            chunks.push(chunk);
        }
        const buffer = Buffer.concat(chunks);

        let df; // This will simulate a DataFrame (array of objects)

        if (fileKey.toLowerCase().endsWith('.csv')) {
            const results = [];
            const readableStream = Readable.from(buffer.toString('utf-8'));
            await new Promise((resolve, reject) => {
                readableStream
                    .pipe(csv())
                    .on('data', (data) => results.push(data))
                    .on('end', () => resolve())
                    .on('error', (err) => reject(err));
            });
            df = results;
        } else if (fileKey.toLowerCase().endsWith('.xlsx')) {
            const workbook = XLSX.read(buffer, { type: 'buffer' });
            const sheetName = workbook.SheetNames[0]; // Assume first sheet
            df = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);
        } else {
            console.error(`Unsupported file format for S3 key: ${fileKey}. Only .csv and .xlsx are supported.`);
            return false;
        }

        if (!df || df.length === 0) {
            console.error("No data parsed from file.");
            return false;
        }

        // Filter out rows with missing 'Rollno', 'Name', or 'Course Name'
        const filteredDf = df.filter(row =>
            row['Rollno'] !== undefined && row['Rollno'] !== null && row['Rollno'] !== '' &&
            row['Name'] !== undefined && row['Name'] !== null && row['Name'] !== '' &&
            row['Course Name'] !== undefined && row['Course Name'] !== null && row['Course Name'] !== ''
        );

        if (filteredDf.length === 0) {
            console.error("No valid data rows after filtering for missing values.");
            return false;
        }

        // Populate studentSubjectsMap and subjectToStudentsMap
        for (const row of filteredDf) {
            const rollno = String(row['Rollno']); // Ensure string type
            const name = String(row['Name']);
            const courseName = String(row['Course Name']);

            // Update studentSubjectsMap
            if (!studentSubjectsMap.has(rollno)) {
                studentSubjectsMap.set(rollno, []);
            }
            studentSubjectsMap.get(rollno).push(courseName);

            // Update subjectToStudentsMap
            if (!subjectToStudentsMap.has(courseName)) {
                subjectToStudentsMap.set(courseName, []);
            }
            const studentsInSubject = subjectToStudentsMap.get(courseName);
            const studentExists = studentsInSubject.some(s => s.rollno === rollno);
            if (!studentExists) {
                studentsInSubject.push({ rollno, name });
            }
        }

        console.log(`Data loaded successfully from S3. Found ${studentSubjectsMap.size} students.`);
        return true;

    } catch (error) {
        if (error instanceof NoCredentialsError) {
            console.error("Error: AWS credentials not found. Please configure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.");
        } else if (error instanceof ClientError) {
            const error_code = error.name; // Use error.name for AWS SDK v3
            if (error_code === 'NoSuchKey') {
                console.error(`Error: S3 object '${fileKey}' not found in bucket '${bucketName}'. Check file key and bucket name.`);
            } else if (error_code === 'AccessDenied') {
                console.error(`Error: Access denied to S3 bucket '${bucketName}' or object '${fileKey}'. Check IAM permissions and bucket policy.`);
            } else {
                console.error(`Error accessing S3: ${error.message}`);
            }
        } else {
            console.error(`Error loading data from S3: ${error.message}`);
        }
        return false;
    }
}

/**
 * Builds the conflict graph where nodes are subjects and edges represent conflicts.
 * A conflict exists if at least one student is registered for both subjects.
 * @returns {Map<string, Set<string>>} The adjacency list representation of the conflict graph.
 */
function buildConflictGraph() {
    const conflictGraph = new Map(); // Adjacency list: Map<Subject, Set<ConflictingSubject>>

    if (studentSubjectsMap.size === 0) {
        console.error("Error: Student-subject map is empty. Load data first.");
        return conflictGraph;
    }

    const allSubjects = new Set();
    for (const subjects of studentSubjectsMap.values()) {
        subjects.forEach(subject => allSubjects.add(subject));
    }

    // Initialize graph with all subjects as nodes
    allSubjects.forEach(subject => conflictGraph.set(subject, new Set()));

    // Identify conflicts: if a student takes two subjects, add an edge between them
    for (const subjects of studentSubjectsMap.values()) {
        for (let i = 0; i < subjects.length; i++) {
            for (let j = i + 1; j < subjects.length; j++) {
                const subject1 = subjects[i];
                const subject2 = subjects[j];
                if (subject1 !== subject2) {
                    conflictGraph.get(subject1).add(subject2);
                    conflictGraph.get(subject2).add(subject1);
                }
            }
        }
    }

    console.log(`Conflict graph built with ${conflictGraph.size} subjects and ${[...conflictGraph.values()].reduce((sum, set) => sum + set.size, 0) / 2} conflicts.`);
    return conflictGraph;
}

/**
 * Implements a greedy graph coloring algorithm.
 * Assigns a color (slot number) to each subject such that no two conflicting subjects have the same color.
 * @param {Map<string, Set<string>>} graph The conflict graph as an adjacency list.
 * @returns {Map<string, number>} A map from subject name to its assigned slot index (color).
 */
function greedyColoring(graph) {
    const coloring = new Map(); // Map<Subject, SlotIndex>
    const subjects = Array.from(graph.keys());

    // Sort subjects by degree (number of conflicts) in descending order
    subjects.sort((a, b) => graph.get(b).size - graph.get(a).size);

    for (const subject of subjects) {
        const usedColors = new Set();
        // Check colors of neighbors
        for (const neighbor of graph.get(subject)) {
            if (coloring.has(neighbor)) {
                usedColors.add(coloring.get(neighbor));
            }
        }

        // Find the smallest available color
        let color = 0;
        while (usedColors.has(color)) {
            color++;
        }
        coloring.set(subject, color);
    }

    return coloring;
}

/**
 * Generates the examination timetable from the coloring result.
 * @param {Map<string, number>} coloring A map from subject name to its assigned slot index.
 * @param {Map<string, Array<{rollno: string, name: string}>>} subjectToStudentsMap Map of subjects to their enrolled students.
 * @returns {Array<Object>} Formatted timetable with subjects, slots, and student details.
 */
function generateTimetableSlots(coloring, subjectToStudentsMap) {
    const formattedTimetable = [];

    for (const [subject, slotIndex] of coloring.entries()) {
        const dayNum = Math.floor(slotIndex / SLOTS_PER_DAY) + 1;
        const slotInDay = (slotIndex % SLOTS_PER_DAY) + 1;
        const slotName = `Day-${dayNum} Slot-${slotInDay}`;

        const studentsForSubject = subjectToStudentsMap.get(subject) || [];

        formattedTimetable.push({
            subject: subject,
            slot: slotName,
            students: studentsForSubject,
        });
    }
    console.log("Timetable generated successfully.");
    return formattedTimetable;
}

// --- API Endpoints ---

app.get('/', (req, res) => {
    res.send("Welcome to the Examination Timetable API (Node.js)! Use /generate_timetable, /convert_xlsx_to_csv, or /upload_xlsx_to_s3.");
});

app.get('/generate_timetable', async (req, res) => {
    const { bucket_name, file_key } = req.query;

    if (!bucket_name || !file_key) {
        return res.status(400).json({
            status: "error",
            message: "Missing 'bucket_name' or 'file_key' query parameters."
        });
    }

    if (!AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY) {
        return res.status(500).json({
            status: "error",
            message: "AWS credentials not configured on the server. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables."
        });
    }

    // Load data from S3
    const dataLoaded = await loadDataFromS3(bucket_name, file_key);
    if (!dataLoaded) {
        return res.status(500).json({
            status: "error",
            message: "Failed to load data from S3. Check server logs, S3 configuration, and provided bucket/key."
        });
    }

    // Build conflict graph
    const conflictGraph = buildConflictGraph();
    if (conflictGraph.size === 0) {
        return res.status(500).json({
            status: "error",
            message: "Failed to build conflict graph (no subjects or valid data). Check server logs for details."
        });
    }

    // Generate timetable
    const coloring = greedyColoring(conflictGraph);
    const timetable = generateTimetableSlots(coloring, subjectToStudentsMap);

    res.json({
        status: "success",
        message: "Timetable generated successfully.",
        timetable: timetable,
        notes: [
            `This timetable uses abstract 'Day-X Slot-Y' assignments.`,
            `The number of slots per day is configured as ${SLOTS_PER_DAY}.`,
            `No student will have two exams in the same slot.`,
            `All campuses are assumed to have the exam for a given subject on the same day and slot.`
        ]
    });
});

app.post('/convert_xlsx_to_csv', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ status: "error", message: "No file part in the request" });
    }

    if (!req.file.originalname.toLowerCase().endsWith('.xlsx')) {
        return res.status(400).json({ status: "error", message: "Invalid file type. Only .xlsx files are supported." });
    }

    try {
        const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0]; // Assume first sheet
        const csvContent = XLSX.utils.sheet_to_csv(workbook.Sheets[sheetName]);

        res.setHeader('Content-disposition', 'attachment; filename=converted_data.csv');
        res.setHeader('Content-type', 'text/csv');
        res.send(csvContent);
    } catch (error) {
        console.error(`Error during XLSX to CSV conversion: ${error.message}`);
        res.status(500).json({ status: "error", message: `Error processing file: ${error.message}` });
    }
});

app.post('/upload_xlsx_to_s3', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ status: "error", message: "No file part in the request" });
    }

    if (!req.file.originalname.toLowerCase().endsWith('.xlsx')) {
        return res.status(400).json({ status: "error", message: "Invalid file type. Only .xlsx files are supported for upload." });
    }

    if (!UPLOAD_S3_BUCKET_NAME || UPLOAD_S3_BUCKET_NAME === 'your-upload-s3-bucket') {
        return res.status(500).json({ status: "error", message: "S3 upload bucket not configured on the server. Please set UPLOAD_S3_BUCKET_NAME environment variable." });
    }

    if (!AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY) {
        return res.status(500).json({
            status: "error",
            message: "AWS credentials not configured on the server. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables."
        });
    }

    try {
        const originalFilename = req.file.originalname;
        const fileExtension = originalFilename.split('.').pop();
        const uniqueFilename = `${uuidv4()}.${fileExtension}`;
        const s3FileKey = `uploads/${uniqueFilename}`; // Customize S3 path as needed

        const uploadParams = {
            Bucket: UPLOAD_S3_BUCKET_NAME,
            Key: s3FileKey,
            Body: req.file.buffer, // Use the buffer from multer
            ContentType: req.file.mimetype // Set content type from multer
        };

        await s3Client.send(new PutObjectCommand(uploadParams));

        console.log(`File '${originalFilename}' uploaded to s3://${UPLOAD_S3_BUCKET_NAME}/${s3FileKey}`);

        res.json({
            status: "success",
            message: "File uploaded to S3 successfully.",
            bucket_name: UPLOAD_S3_BUCKET_NAME,
            file_key: s3FileKey
        });

    } catch (error) {
        if (error instanceof NoCredentialsError) {
            console.error("Error: AWS credentials not found for S3 upload.");
            res.status(500).json({ status: "error", message: "AWS credentials not configured on the server." });
        } else if (error instanceof ClientError) {
            const error_code = error.name;
            console.error(`S3 ClientError during upload: ${error_code} - ${error.message}`);
            if (error_code === 'AccessDenied') {
                res.status(500).json({ status: "error", message: "S3 access denied. Check IAM permissions for bucket." });
            } else {
                res.status(500).json({ status: "error", message: `S3 upload failed: ${error.message}` });
            }
        } else {
            console.error(`An unexpected error occurred during S3 upload: ${error.message}`);
            res.status(500).json({ status: "error", message: `An unexpected error occurred: ${error.message}` });
        }
    }
});

// --- Start the Server ---
app.listen(port, () => {
    console.log(`Node.js Exam Timetable API listening at http://localhost:${port}`);
    console.log(`
    --- Deployment Instructions for Ubuntu Server ---
    1.  Save this code as 'server.js' (or 'app.js') in your project directory.
    2.  Create a 'package.json' file (see below) and install dependencies:
        npm install
    3.  Set environment variables on your server (e.g., in your Systemd service file):
        AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, UPLOAD_S3_BUCKET_NAME
    4.  Configure a process manager (like PM2) or Systemd to run 'node server.js'.
    5.  Set up Nginx as a reverse proxy to forward requests to port ${port}.
    `);
});