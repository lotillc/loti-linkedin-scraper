import winston from "winston";

const { createLogger, format, transports } = winston;

export const logger = createLogger({
  level: "info",
  format: format.combine(format.timestamp(), format.json()),
  transports: [
    new transports.Console(),
    new transports.File({ filename: "logs/app.log" }),
  ],
});

export const getLogger = (filename) => {
  return createLogger({
    level: "info",
    format: format.combine(format.timestamp(), format.json()),
    transports: [
      new transports.Console(),
      new transports.File({ filename: `logs/${filename}.log` }),
    ],
  });
};
