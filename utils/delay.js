export const delay = (
  baseDelay,
  retries,
  minDiff = 2000,
  minDelay = 1000,
  maxDelay = 6000,
) => {
  let delayTimeWithJitter =
    baseDelay * Math.pow(2, retries) + Math.random() * 1000;

  if (delayTimeWithJitter < minDelay) {
    delayTimeWithJitter = minDelay;
  }

  if (delayTimeWithJitter > maxDelay) {
    delayTimeWithJitter = maxDelay;
  }

  delayTimeWithJitter += Math.random() * minDiff;
  return new Promise((resolve) => setTimeout(resolve, delayTimeWithJitter));
};
