import { fromCSVStream } from "arquero";
import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { parquetWriteBuffer, type ColumnSource, type SchemaElement } from "hyparquet-writer";

async function convertToParquet(inputFilePath: string, outputFilePath: string) {
  const inputFile = Bun.file(inputFilePath);
  const outputFile = Bun.file(outputFilePath);

  const textStream = inputFile.stream().pipeThrough(new TextDecoderStream());
  const dt = await fromCSVStream(textStream, { header: true });
  console.log("input size:", inputFile.size, dt.numRows(), dt.numCols());

  const schema: SchemaElement[] = [];
  const columnData: ColumnSource[] = [];
  for (const columnName of dt.columnNames()) {
    schema.push({
      name: columnName,
    });
  }
  for (const column of schema) {
    columnData.push({
      name: column.name,
      data: dt.column(column.name) as any[],
    });
  }

  const arrayBuffer = parquetWriteBuffer({
    columnData,
    compressed: true,
    statistics: false,
  });
  await outputFile.write(arrayBuffer);
  console.log("output size:", outputFile.size, schema.length);
}

if (import.meta.main) {
  // https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020/data
  // https://creativecommons.org/publicdomain/zero/1.0/
  // await convertToParquet(
  //   "./data/rohanrao--formula-1-world-championship-1950-2020/results.csv",
  //   "./data/f1-1950-2020-results.parquet",
  // );
  // await convertToParquet(
  //   "./data/rohanrao--formula-1-world-championship-1950-2020/races.csv",
  //   "./data/f1-1950-2020-races.parquet",
  // );

  const file = await Bun.file("./data/f1-1950-2020-results.parquet").arrayBuffer();
  const data = await parquetReadObjects({ file, compressors });
  console.log(data.length, data.slice(0, 1));
}
