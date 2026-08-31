const path = require("path");

const {
  cleanBdoExcelFile
} = require("../services/fileCleaners/bdoFileCleaner");

async function run() {
  const inputArgument = process.argv[2];

  if (!inputArgument) {
    console.error(
      "Debes indicar la ruta del archivo BDO .xlsx"
    );

    console.error(
      'Ejemplo: node scripts/testBdoCleaner.js "uploads/preparations/UUID/bdo-original.xlsx"'
    );

    process.exitCode = 1;
    return;
  }

  const inputPath = path.resolve(
    inputArgument
  );

  console.log(
    "Limpiando archivo BDO:"
  );

  console.log(inputPath);

  try {
    const result =
      await cleanBdoExcelFile(inputPath);

    console.log(
      "\nArchivo generado correctamente:"
    );

    console.log(result.outputPath);

    console.log(
      "\nEstadísticas:"
    );

    console.table(
      result.statistics
    );
  } catch (error) {
    console.error(
      "\nError limpiando el archivo:"
    );

    console.error(error.message);

    process.exitCode = 1;
  }
}

run();