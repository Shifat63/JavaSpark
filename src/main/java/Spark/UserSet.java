package Spark;

import java.util.HashSet;
import java.util.Set;

public class UserSet {
    public Set<String> userSet;
    public UserSet(Set<String> userSet){
        if(userSet==null){
            this.userSet = new HashSet<String>();
        }
        else
        {
            this.userSet = userSet;
        }
    }

    public Set<String> add(String username)
    {
        this.userSet.add(username);
        return this.userSet;
    }

    public Set<String> add (Set<String> set){
        this.userSet.addAll(set);
        return this.userSet;
    }

    public double distanceTo(Set<String> other){
        Set<String> union = new HashSet<>(this.userSet);
        union.addAll(other);
        Set<String> intersect = new HashSet<>(this.userSet);
        intersect.retainAll(other);
        if(union.size() > 0){
            return (double)intersect.size()/(double)union.size();
        }
        else
            return 0;
    }
}
