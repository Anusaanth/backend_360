var express = require('express');
var router = express.Router();
const multer = require('multer')
const fs = require("fs")
const { promisify } = require("util");
const pipeline = promisify(require("stream").pipeline);
const path = require('path');
const { spawn } = require('child_process');
const R = require('r-integration');

/* GET home page. */
router.get('/', function (req, res, next) {
  res.render('index', { title: 'Express' });
});


const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    return cb(null, "./Model/upload_file")
  },
  filename: function (req, file, cb) {
    return cb(null, `${Date.now()}_${file.originalname}`);
  }
})

const upload = multer()
router.post("/upload", upload.single("file"), async function (req, res, next) {
  // console.log(req.file)
  try {
    const uploadedFile = req.file.buffer;
    const xlsxFilePath = path.join(__dirname, 'Model', 'Python file for Cartofact data extracting', 'lic.xlsx');
    fs.writeFileSync(xlsxFilePath, uploadedFile, 'utf8');

    console.log(__dirname)
    // const rScriptPath = path.join(__dirname, 'Model', 'R code model files', 'test.R');
    // const rProcess = spawn('Rscript', [rScriptPath]);


    const rScriptPath = path.join(__dirname, 'Model', 'R code model files', 'test.R');
    console.log(rScriptPath)
    // let result = R.executeRScript(rScriptPath);



    const csvPath = path.join(__dirname, 'Model', 'excel data files for R code models', 'lic.csv');
    console.log(csvPath)
    const csvData = fs.readFileSync(csvPath, 'utf8');
    res.attachment('lic.csv');
    res.send(csvData);

  } catch (error) {
    console.error('Error executing R script:', error.message);
    console.error('Error stack trace:', error.stack);
  }



})


// router.post('/testUpload', function (req, res, next) {
//   try {
//     console.log(req.file);  // Check if the file is in the request body
//     res.send('File uploaded successfully')

//   } catch (error) {
//     console.error('Error handling file upload:', error);
//     res.status(500).send('Internal Server Error');
//   }
// });

module.exports = router;
