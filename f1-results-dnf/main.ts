import { ColumnTable, fromArrow, fromArrowStream, fromCSVStream, table, toArrowIPC } from "arquero";
import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { parquetWriteBuffer, type ColumnSource, type SchemaElement } from "hyparquet-writer";

export async function readCsvTable(inputFile: Bun.BunFile): Promise<ColumnTable> {
  const inputStream = inputFile.stream().pipeThrough(new TextDecoderStream());
  return fromCSVStream(inputStream, { header: true });
}

export async function readArrowTable(inputFile: Bun.BunFile): Promise<ColumnTable> {
  const inputBuffer = await inputFile.arrayBuffer();
  const arrowBuffer = await Bun.zstdDecompress(inputBuffer);
  return fromArrow(arrowBuffer);
}

export async function writeArrowTable(outputFile: Bun.BunFile, table: ColumnTable) {
  const arrowBuffer = toArrowIPC(table, { format: "file" });
  const outputBuffer = await Bun.zstdCompress(arrowBuffer, { level: 5 });
  await outputFile.write(outputBuffer);
}

export async function readParquetData(inputFile: Bun.BunFile) {
  const inputBuffer = await inputFile.arrayBuffer();
  const objects = await parquetReadObjects({
    file: inputBuffer,
    compressors,
  });
  return objects;
}

export async function writeParquetData(outputFile: Bun.BunFile, table: ColumnTable) {
  const columnData = await toParquetColumnData(table);
  const outputBuffer = parquetWriteBuffer({
    columnData: columnData.columnData,
    compressed: true,
    statistics: false,
  });
  await outputFile.write(outputBuffer);
}

export async function toParquetColumnData(dt: ColumnTable) {
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

  return { schema, columnData };
}

if (import.meta.main) {
  // https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020/data
  // https://creativecommons.org/publicdomain/zero/1.0/

  // parquet.
  await writeParquetData(
    Bun.file("./data/f1-1950-2020-results.parquet"),
    await readCsvTable(
      Bun.file("./data/rohanrao--formula-1-world-championship-1950-2020/results.csv"),
    ),
  );
  await writeParquetData(
    Bun.file("./data/f1-1950-2020-races.parquet"),
    await readCsvTable(
      Bun.file("./data/rohanrao--formula-1-world-championship-1950-2020/races.csv"),
    ),
  );

  // arrow.
  await writeArrowTable(
    Bun.file("./data/f1-1950-2020-results.zstd.arrow"),
    await readCsvTable(
      Bun.file("./data/rohanrao--formula-1-world-championship-1950-2020/results.csv"),
    ),
  );
  await writeArrowTable(
    Bun.file("./data/f1-1950-2020-races.zstd.arrow"),
    await readCsvTable(
      Bun.file("./data/rohanrao--formula-1-world-championship-1950-2020/races.csv"),
    ),
  );
}
