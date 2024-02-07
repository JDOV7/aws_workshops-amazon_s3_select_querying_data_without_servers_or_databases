import { S3Client, SelectObjectContentCommand } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
dotenv.config();
const { accessKeyId, secretAccessKey, Bucket } = process.env;
const client = new S3Client({
  credentials: {
    accessKeyId,
    secretAccessKey,
  },
});

const buscarElementos = async (configuracion = {}) => {
  const comando = new SelectObjectContentCommand(configuracion);
  const respuesta = await client.send(comando);
  let results = "";

  for await (const event of respuesta.Payload) {
    if (event.Records) {
      const chunk =
        event.Records.Payload instanceof Buffer
          ? event.Records.Payload
          : Buffer.from(event.Records.Payload);

      results += chunk.toString();
    }
  }
  const jsonArray = results.trim().split("\n");
  return jsonArray;
};

const delimitarTexto = (text = "", delimiter = ",") => {
  return text.split(delimiter);
};

const convertirCSVaJSON = (headers = [], body = []) => {
  const respuesta = [];
  body.forEach((elemento) => {
    const jsonRes = {};
    const elementoToArray = delimitarTexto(elemento, ",");
    for (let i = 0; i < headers.length; i++) {
      jsonRes[headers[i]] = elementoToArray[i];
    }
    respuesta.push(jsonRes);
  });
  return respuesta;
};

const handler = async (event, context) => {
  try {
    const { Name, PhoneNumber, City, Occupation } = event;
    // const paramsEle = event.Name || "X";

    const arrCondiciones = [];

    if (Name != "") {
      arrCondiciones.push(`s._1 ='${Name}'`);
    }
    if (PhoneNumber != "") {
      arrCondiciones.push(`s._2 ='${PhoneNumber}'`);
    }

    if (City != "") {
      arrCondiciones.push(`s._3 ='${City}'`);
    }

    if (Occupation != "") {
      arrCondiciones.push(`s._4 ='${Occupation}'`);
    }

    let sentenciaSQL = "SELECT * FROM s3object s ";

    for (let index = 0; index < arrCondiciones.length; index++) {
      const element = arrCondiciones[index];
      if (index == 0) {
        sentenciaSQL += ` WHERE ${element}`;
      } else {
        sentenciaSQL += ` AND ${element}`;
      }
    }

    const configuracion = {
      Bucket,
      Key: "sample_data.csv",
      Expression: "select * from s3object s limit 1", //OBTENIENDO LOS ENCABEZADOS
      ExpressionType: "SQL",
      InputSerialization: {
        CSV: {
          FileHeaderInfo: "NONE",
          // Type: "DOCUMENT",
        },
      },
      OutputSerialization: { CSV: {} },
    };
    const headers = await buscarElementos(configuracion);
    const headersArr = delimitarTexto(headers[0]);
    configuracion.Expression = sentenciaSQL;
    // configuracion.InputSerialization.CSV.FileHeaderInfo = "USE";
    const body = await buscarElementos(configuracion);
    const conversion = convertirCSVaJSON(headersArr, body);

    const response = {
      statusCode: 200,
      body: {
        conversion,
        params: {
          sentenciaSQL,
          Name,
          PhoneNumber,
          City,
          Occupation,
        },
      },
    };
    return response;
  } catch (error) {
    const response = {
      statusCode: 200,
      body: { error: error.message, error2: error },
    };
    return response;
  }
};


export { handler };
