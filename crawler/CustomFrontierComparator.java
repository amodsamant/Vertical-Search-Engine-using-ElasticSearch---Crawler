package crawler;

import java.util.Comparator;

import model.UrlData;

public class CustomFrontierComparator implements Comparator<UrlData> {

	public int compare(UrlData arg0, UrlData arg1) {
		if(arg0.getLevel()==arg1.getLevel()) {
			if(arg0.getInlinksCount()==arg1.getInlinksCount()) {
				if(arg0.getTimeOfInsertion() < arg1.getTimeOfInsertion()) {
					return -1;
				} else {
					return 1;
				}
			} else if (arg0.getInlinksCount()>arg1.getInlinksCount()) {
				return -1;
			}
			else 
				return 1;
		} else {
			if(arg0.getLevel()<arg1.getLevel()) {
				return -1;
			} else {
				return 1;
			}
		} 
	}
	
}