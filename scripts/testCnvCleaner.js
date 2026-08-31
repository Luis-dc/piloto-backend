const path = require("path");

const {
  cleanCnvExcelFile
} = require("../services/fileCleaners/cnvFileCleaner");

async function run() {
  const inputArgument = process.argv[2];

  if (!inputArgument) {
    console.error(
      "Debes indicar la ruta del archivo 2CNV .xlsx"
    );

    console.error(
      'Ejemplo: node scripts/testCnvCleaner.js "uploads/preparations/UUID/cnv-original.xlsx"'
    );

    process.exitCode = 1;
    return;
  }

  const inputPath = path.resolve(
    inputArgument
  );

  console.log(
    "Limpiando archivo 2CNV:"
  );

  console.log(inputPath);

  try {
    const result =
      await cleanCnvExcelFile(inputPath);

    console.log(
      "\nArchivo generado correctamente:"
    );

    console.log(result.outputPath);

    console.log(
      "\nEstadísticas:"
    );

    const {
        invalidBalanceSamples = [],
        ...summaryStatistics
      } = result.statistics;
      
      console.table(summaryStatistics);
      
      if (invalidBalanceSamples.length > 0) {
        console.log(
          "\nEjemplos de saldos inválidos:"
        );
      
        console.table(
          invalidBalanceSamples
        );
      }
  } catch (error) {
    console.error(
      "\nError limpiando el archivo:"
    );

    console.error(error.message);

    process.exitCode = 1;
  }
}

run();