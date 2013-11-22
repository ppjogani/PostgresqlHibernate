/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */


package edu.usc.bg.generator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Vector;


/**
 * Generates a zipfian distribution
 * @author barahman
 *
 */
public class DistOfAccess {
	Random randNumGenerator;
	String currDist;
	double ZipfianMean = 0.27;
	static int numberOfUsers;
	static Double[] DistLevelsArray = null;
	static Vector<Double> DistValues;
	Vector<Integer> SV;
	int SV_Length;
	boolean MakeRec=false;
	double nTime;
	boolean bBinarySearch; //  0 for linear, 1 for binary

	int LinearSearch(int nNum)
	{
		int randMovie = 0;
		for (int i=0; i<=numberOfUsers; i++)
		{
			if (DistLevelsArray[i] > nNum)
			{
				randMovie = i;
				break;
			}
		}
		return randMovie;
	}


	int BinarySearch(int nNum, int nStart, int nEnd)
	{
		int nIndex = (nEnd-nStart)/2;
		nIndex+=nStart;
		if(DistLevelsArray[nIndex] <= nNum && DistLevelsArray[nIndex+1] > nNum)
			return nIndex+1;
		else if(DistLevelsArray[nIndex] >= nNum && DistLevelsArray[nIndex+1] >= nNum)
			return BinarySearch(nNum, nStart, nIndex);
		else if(DistLevelsArray[nIndex] <= nNum && DistLevelsArray[nIndex+1] <= nNum)
			return BinarySearch(nNum, nIndex+1, nEnd);
		else
			return nEnd;
	}


	public void reWriteProbs(Cluster  myCluster){
		Double[] tmpDistLevelsArray = new Double[myCluster.members.size()+1];
		Vector<Double> tmpDistValues = new Vector<Double>();
		tmpDistLevelsArray[0] = 0.0;
		tmpDistValues.add(0.0);
		for(int i=1; i<=myCluster.members.size(); i++){
			if ( currDist.equals("Zipfian") )
				tmpDistValues.add(DistValues.get(myCluster.members.get(i-1).getUserid()+1));
			else 
				tmpDistValues.add(10.0);
			tmpDistLevelsArray[i] = tmpDistLevelsArray[i-1] + tmpDistValues.get(i);
		}

		DistLevelsArray = new Double[myCluster.members.size()+1];
		DistValues = new Vector<Double>();
		DistLevelsArray = tmpDistLevelsArray;
		DistValues = tmpDistValues;		
		setNumUsers(myCluster.members.size());
	}
	/*public void reWriteProbs(Double[] newProbs)
	{
		Double[] tmpDistLevelsArray = new Double[numberOfUsers+1];
		Vector<Double> tmpDistValues = new Vector<Double>();
		tmpDistLevelsArray[0] = 0.0;
		tmpDistValues.add(0.0);
		for (int i=1; i<=newProbs.length; i++)
		{
			if ( currDist.equals("Zipfian") )
				tmpDistValues.add(100*newProbs[i-1]);
			else 
				tmpDistValues.add(10.0);
			tmpDistLevelsArray[i] = tmpDistLevelsArray[i-1] + tmpDistValues.get(i);
		}	

		DistLevelsArray = new Double[numberOfUsers+1];
		DistValues = new Vector<Double>();
		DistLevelsArray = tmpDistLevelsArray;
		DistValues = tmpDistValues;

	}*/


	void InitZipfian(int numOfItems, double ZipfMean)
	{
		numberOfUsers = numOfItems;
		ZipfianMean = ZipfMean;

		DistLevelsArray = new Double[numOfItems+1];

		SV_Length = numOfItems+1;
		SV = new Vector<Integer>(SV_Length);

		//initialize the prob vector
		for (int i = 0 ; i < numOfItems+1 ; i++) 
		{
			SV.add(0);
		}

		DistValues = new Vector<Double>();
		DistLevelsArray[0] = 0.0;

		DistValues.add(0.0);
		//keeping a normalized value of the distances for each rank(userid)
		for (int i=1; i<=numberOfUsers; i++)
		{
			if ( currDist.equals("Zipfian") ){
				DistValues.add(100 * Math.pow(i, -(1-ZipfianMean))/Math.pow(numberOfUsers, -(1-ZipfianMean)));
			}
			else 
				DistValues.add(10.0);
			DistLevelsArray[i] = DistLevelsArray[i-1] + DistValues.get(i);
		}	
	}



	public DistOfAccess(int numOfItems, String distname, boolean bBinary, double ZipfianMean)
	{
		if ( distname.equals("U")  || 
				distname.equals("u")  || 
				distname.equals("Uniform")  || 
				distname.equals("Unif")  || 
				distname.equals("uniform")  || 
				distname.equals("UNIFORM")  || 
				distname.equals("UNIF")  )
			currDist="Uniform";
		else 
			currDist = "Zipfian";
		bBinarySearch = bBinary;
		randNumGenerator = new Random();

		InitZipfian(numOfItems, ZipfianMean);

	}

	public DistOfAccess(int numOfItems, String distname, boolean bBinary, 
			double ZipfianMean, int randomSeed)
	{
		if ( distname.equals("U")  || 
				distname.equals("u")  || 
				distname.equals("Uniform")  || 
				distname.equals("Unif")  || 
				distname.equals("uniform")  || 
				distname.equals("UNIFORM")  || 
				distname.equals("UNIF")  )
			currDist="Uniform";
		else 
			currDist = "Zipfian";
		bBinarySearch = bBinary;
		randNumGenerator = new Random(randomSeed);
		InitZipfian(numOfItems, ZipfianMean);
	}

	int getRandomNum( int max )
	{
		return randNumGenerator.nextInt(max);  //should switch to java.util.concurrent.*;  ThreadLocalRandom.current() .nextInt
		//return ThreadLocalRandom.current().nextInt(max);
	}

	public int GenerateOneItem()
	{
		int randMovie = 0;
		int max = (int)(double)DistLevelsArray[numberOfUsers];
		int movieIndex = getRandomNum( max );
		Integer temp_val;

		if(!bBinarySearch)
			randMovie = LinearSearch(movieIndex);
		else
			randMovie = BinarySearch(movieIndex, 0 , numberOfUsers);

		if (MakeRec)
		{
			if (randMovie >= 0 && randMovie <= SV_Length)
			{
				temp_val = SV.get(randMovie);
				SV.set(randMovie, temp_val + 1);
			}
			else 
			{
				System.out.println("Error in DistOfAccess.cs, indexing item " + randMovie + " which is out of range.");
			}
		}
		return randMovie;
	}

	//index starts from one
	static double GetProbability(int index)
	{
		if (index < 1 || index > numberOfUsers) 
			return -1;
		return (double)DistValues.get(index) / (double)DistLevelsArray[numberOfUsers];
	}

	void PrintAccurracy ()
	{
		if (MakeRec)
		{
			System.out.println("Item \t Obs Freq \t Exp Freq \t Freq Err");
			int TotalSamples = 0;
			for (int i = 0; i < numberOfUsers+1 ; i++)
				TotalSamples += SV.get(i);
			if (TotalSamples > 0)
			{
				double ObsFreq = 0.0;
				for (int i = 1; i < numberOfUsers+1 ; i++)
				{
					ObsFreq = (double) SV.get(i) / TotalSamples;
					System.out.println( i + " \t " + ObsFreq + " \t " + GetProbability(i) + " \t " + 
							((double) 100 * (GetProbability(i) - ObsFreq ) / GetProbability(i)));

				}
			} 
			else 
				System.out.println("Error, total samples is " + TotalSamples);
		} 
		else 
		{

			System.out.println("Error, MakeRecording was not enabled.\n" +
					"Enable MakeRecording must be enabled to gather statistics.\n" +
					"Usage:  DistOfAccess.MakeRecording = true");
		}
	}


	void setNumUsers(int newNumUsers){
		numberOfUsers = newNumUsers;
	}

	public static void main( String[] args )
	{
		int num_items = 10;
		int num_runs = 10000;
		String distrib_name = "Z";		// "U" (uniform) or "Z" (zipfian)

		DistOfAccess dist = new DistOfAccess(num_items,distrib_name,true,0.27);
		int [] count_array = new int[num_items];

		// Keep track of the distribution of generated items
		for( int i = 0; i < num_runs; i++ )
		{
			// minus 1 because dist generates from 1 to num_items
			count_array[dist.GenerateOneItem() - 1]++;
		}

		try {
			String fileName = "CentZipfian"+num_items+"-"+num_runs+".txt";
			FileWriter fstream = new FileWriter(fileName);
			BufferedWriter out = new BufferedWriter(fstream);
			//print the frequencies
			// Display final distribution
			for( int i = 0; i < num_items; i++ )
			{
				System.out.println(i+1 + ", " + count_array[i] );
				out.write((i+1) + ", " + count_array[i]+"\n");
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
