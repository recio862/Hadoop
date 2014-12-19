package com.refactorlabs.cs378;

/**
 * @author Renato John Recio
 * 
 * 
 * ComparableImpression class stores an impression object and implements the comparable
 * interface. CompareTo function uses the impressions TimeStamp information
 * to determine the sort order for impressions in the session object.
 */
public class ComparableImpression implements Comparable<ComparableImpression> {

	//Private impression for comparator
	private Impression myImp; 


	//Constructor sets private impression
	public ComparableImpression(Impression newImp){
		setMyImp(newImp);
	}

	//Getter for impression
	public Impression getMyImp() {
		return myImp;
	}

	//Setter for impression
	public void setMyImp(Impression myImp) {
		this.myImp = myImp;
	}

	//Comparator for ComparableImpression object - checks argument 
	//against internal impression
	@Override
	public int compareTo(ComparableImpression o) {
		ComparableImpression imp = (ComparableImpression) o;

		//If our stored impression has a larger timestamp than the argument, return 1
		//else, return -1
		if (myImp.getTimestamp() > imp.getMyImp().getTimestamp())
			return 1;
//		else 
//			return -1;
		else if (myImp.getTimestamp() < imp.getMyImp().getTimestamp())
			return -1;
		else 
			return 0;

	}



}
