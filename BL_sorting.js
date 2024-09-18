const fs = require('fs');
const path = require('path');
const fastcsv = require('fast-csv');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');

// Specify the directory containing your CSV files
const directoryPath = '/Users/antonshelkovnikov/Desktop/BL_Sorting'; // Replace with your directory path
const outputFile = 'combined.csv'; // Output file name


// Function to capitalize the first letter of a string
function capitalizeFirstLetter(name) {
    if (!name) return '';
    return name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
}



async function processCsvFiles(directory) {
    const data = [];
    const emailSet = new Set();

    const files = fs.readdirSync(directory).filter(file => path.extname(file).toLowerCase() === '.csv');

    for (const file of files) {
        const filePath = path.join(directory, file);
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (row) => {
                    // Keep only the required columns
                    const filteredRow = {
                        'Email Address': row['Email Address'],
                        'First Name': capitalizeFirstLetter(row['First Name']),
                        'Last Name': capitalizeFirstLetter(row['Last Name'])
                    };

                    if (!emailSet.has(filteredRow['Email Address'])) {
                        emailSet.add(filteredRow['Email Address']);
                        data.push(filteredRow);
                    }
                })
                .on('end', resolve)
                .on('error', reject);
        });
    }

    // Write combined data to a new CSV file
    const csvWriter = createObjectCsvWriter({
        path: outputFile,
        header: [
            { id: 'Email Address', title: 'Email Address' },
            { id: 'First Name', title: 'First Name' },
            { id: 'Last Name', title: 'Last Name' }
        ]
    });

    await csvWriter.writeRecords(data);
    console.log('CSV files have been combined and duplicates removed.');
}

processCsvFiles(directoryPath).catch(console.error);