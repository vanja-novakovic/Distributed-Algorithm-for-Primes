using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Http;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using System.Numerics;
using System.Collections.Generic;
using System.Linq;

namespace DAforPrimes
{
    public static class DistributedPrimesAlgorithm
    {
        [FunctionName("ManageRequest")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string data = await req.Content.ReadAsStringAsync();
            string instanceId = await starter.StartNewAsync("Orchestration", input: data);

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        [FunctionName("Orchestration")]
        public static async Task<BigInteger> RunOrchestrator([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            string data = context.GetInput<string>();
            dynamic jsonInput = JsonConvert.DeserializeObject(data);
            int machineCount = int.Parse((string)jsonInput.machineCount);

            List<Range> ranges = new List<Range>();
            foreach (var range in jsonInput.ranges)
            {
                BigInteger s = BigInteger.Parse((string)range.start);
                BigInteger e = BigInteger.Parse((string)range.end);
                Range temp = new Range(s, e);
                ranges.Add(temp);
            }

            BigInteger sum = 0;
            foreach (var range in ranges)
                sum += range.Count;
       
            BigInteger countPerMachine = (BigInteger)Math.Ceiling((double)sum / (double)machineCount);

            List<Range>[] distributedRanges = new List<Range>[machineCount];
            for (int i = 0; i < machineCount; i++)
            {
                distributedRanges[i] = new List<Range>();
                BigInteger count = 0;
                int addedRangesCount = 0;
                foreach (var range in ranges)
                {
                    if (count + range.Count <= countPerMachine)
                    {
                        distributedRanges[i].Add(range);
                        count += range.Count;
                        addedRangesCount++;
                    }
                    else
                    {
                        BigInteger remainder = countPerMachine - count;
                        distributedRanges[i].Add(range.GetSubrange(remainder));
                        break;
                    }
                }
                ranges.RemoveRange(0, addedRangesCount);
            }

            List<Task<BigInteger>> runningTasks = new List<Task<BigInteger>>();
            foreach (var range in distributedRanges)
            {
                Task<BigInteger> temp = context.CallActivityAsync<BigInteger>("GetPrimes", range);
                runningTasks.Add(temp);
            }

            BigInteger[] primesCounts = await Task.WhenAll(runningTasks);

            BigInteger totalCount = 0;
            foreach (var count in primesCounts)
            {
                totalCount += count;
            }

            PrintRangesDistribution(distributedRanges);
            return totalCount;
        }

        [FunctionName("GetPrimes")]
        public static BigInteger GetPrimeNumbers([ActivityTrigger] List<Range> list, ILogger log)
        {
            BigInteger count = 0;

            foreach (var range in list)
            {
                for (BigInteger i = range.start; i <= range.end; i++)
                {
                    bool isPrime = true;
                    for (BigInteger j = 2; j <= i / 2; j++)
                        if (i % j == 0)
                        {
                            isPrime = false;
                            break;
                        }

                    if (isPrime && i > 1)
                        count++;
                }
            }

            return count;

        }

        public static void PrintRangesDistribution(List<Range>[] distributedRanges)
        {
            int machineId = 1;
            foreach (var r in distributedRanges)
            {
                System.Console.WriteLine("\n" + "Machine Identification Number: " + machineId);
                machineId++;
                BigInteger count = 0;
                System.Console.Write("Set of ranges: {");
                foreach (var element in r)
                {
                    System.Console.Write(" (" + element.start + " - " + element.end + ") ");
                    count += element.end - element.start;
                }
                System.Console.Write("}");
                System.Console.WriteLine("\nCount: " + count + "\n");
            }
        }
    }

    public class Range
    {
        public BigInteger start;
        public BigInteger end;
        public BigInteger Count { get; private set; }

        public Range(BigInteger start, BigInteger end)
        {
            if (start < 0 || end < start)
                throw new ArgumentException("Invalid arguments.");
            this.start = start;
            this.end = end;
            this.Count = end - start;
        }

        public Range GetSubrange (BigInteger offset)
        {
            Range subrange;
            if (offset < 0)
                throw new ArgumentException("Invalid argument.");
            subrange = new Range(start, start + offset);
            start = start + offset + 1;
            Count = end - start;
            return subrange;

        }
    }
}

