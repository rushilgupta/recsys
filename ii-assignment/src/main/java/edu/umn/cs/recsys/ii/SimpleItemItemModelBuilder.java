package edu.umn.cs.recsys.ii;

import com.google.common.collect.ImmutableMap;

import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

import org.grouplens.lenskit.collections.LongUtils;
import org.grouplens.lenskit.core.Transient;
import org.grouplens.lenskit.cursors.Cursor;
import org.grouplens.lenskit.data.dao.ItemDAO;
import org.grouplens.lenskit.data.dao.UserEventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.history.RatingVectorUserHistorySummarizer;
import org.grouplens.lenskit.data.history.UserHistory;
import org.grouplens.lenskit.scored.ScoredId;
import org.grouplens.lenskit.scored.ScoredIdBuilder;
import org.grouplens.lenskit.scored.ScoredIdListBuilder;
import org.grouplens.lenskit.scored.ScoredIds;
import org.grouplens.lenskit.vectors.ImmutableSparseVector;
import org.grouplens.lenskit.vectors.MutableSparseVector;
import org.grouplens.lenskit.vectors.SparseVector;
import org.grouplens.lenskit.vectors.VectorEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author <a href="http://www.grouplens.org">GroupLens Research</a>
 */
public class SimpleItemItemModelBuilder implements Provider<SimpleItemItemModel> {
    private final ItemDAO itemDao;
    private final UserEventDAO userEventDao;
    private static final Logger logger = LoggerFactory.getLogger(SimpleItemItemModelBuilder.class);;

    @Inject
    public SimpleItemItemModelBuilder(@Transient ItemDAO idao,
                                      @Transient UserEventDAO uedao) {
        itemDao = idao;
        userEventDao = uedao;
    }

    @Override
    public SimpleItemItemModel get() {
        // Get the transposed rating matrix
        // This gives us a map of item IDs to those items' rating vectors
        Map<Long, ImmutableSparseVector> itemVectors = getItemVectors();

        // Get all items - you might find this useful
        LongSortedSet items = LongUtils.packedSet(itemVectors.keySet());
        long[] itemArr = items.toLongArray();
        // Map items to vectors of item similarities
        Map<Long,MutableSparseVector> itemSimilarities = new HashMap<Long, MutableSparseVector>();

        // TODO Compute the similarities between each pair of items
        // It will need to be in a map of longs to lists of Scored IDs to store in the model
        for(long itemI: itemArr) {
    		ImmutableSparseVector vectorI = itemVectors.get(itemI);
        	MutableSparseVector similarityForItems = MutableSparseVector.create(items);
        	
        	for(long itemJ: itemArr) {
        		ImmutableSparseVector vectorJ = itemVectors.get(itemJ);
        		if(itemI == itemJ) {
        			continue;
        		}
        		
        		double adjustedCosine = getSimilarity(vectorI, vectorJ);
        		// consider only +ve values
        		if(adjustedCosine > 0) {
        			similarityForItems.set(itemJ, adjustedCosine);
        		}
        	}
    		itemSimilarities.put(itemI, similarityForItems);
        }
        Map<Long,List<ScoredId>> modelMap = buildModelMap(itemSimilarities);
        return new SimpleItemItemModel(modelMap);
    }
    
    /**
     * iterate through a map of vector items and return
     * a map of item mapping to a list of scoredIds sorted in descending order
     */
    private Map<Long,List<ScoredId>> buildModelMap(Map<Long,MutableSparseVector> sparseVectorMap) {
    	Map<Long,List<ScoredId>> map = new HashMap<Long, List<ScoredId>>();
    	
    	for(Entry<Long, MutableSparseVector> entry:sparseVectorMap.entrySet()) {
    		MutableSparseVector vektor = entry.getValue();
    		Iterator<VectorEntry> iterator = vektor.iterator();
    		ScoredIdListBuilder list = ScoredIds.newListBuilder(); 		
    		
    		// build a list of ScoredId for each entry in vector
    		while(iterator.hasNext()) {
    			ScoredIdBuilder builder = ScoredIds.newBuilder();
    			VectorEntry vectorEntry = iterator.next();
    			
    			builder.setId(vectorEntry.getKey());
    			builder.setScore(vectorEntry.getValue());
    			list.add(builder.build());
    		}
    		
    		// sort the items in descending order based on score
    		list.sort(new Comparator<ScoredId>() {
				@Override
				public int compare(ScoredId arg0, ScoredId arg1) {
					if(arg0.getScore() < arg1.getScore()) {
						return 1;
					}
					else if(arg0.getScore() > arg1.getScore()) {
						return -1;
					}
					return 0;
				}
			});
    		map.put(entry.getKey(), list.build());
    	}
    	
    	return map;
    }
    
    /**
     * get cosine for two vectors ( a.b / (|a^2|b^2\) )
     */
    private double getSimilarity(ImmutableSparseVector i, ImmutableSparseVector j)	{
    	return i.dot(j)/(i.norm()*j.norm());    	
    }
    

    /**
     * Load the data into memory, indexed by item.
     * @return A map from item IDs to item rating vectors. Each vector contains users' ratings for
     * the item, keyed by user ID.
     */
    public Map<Long,ImmutableSparseVector> getItemVectors() {
        // set up storage for building each item's rating vector
        LongSet items = itemDao.getItemIds();
        // map items to maps from users to ratings
        Map<Long,Map<Long,Double>> itemData = new HashMap<Long, Map<Long, Double>>();
        for (long item: items) {
            itemData.put(item, new HashMap<Long, Double>());
        }
        // itemData should now contain a map to accumulate the ratings of each item

        // stream over all user events
        Cursor<UserHistory<Event>> stream = userEventDao.streamEventsByUser();
        try {
            for (UserHistory<Event> evt: stream) {
                MutableSparseVector vector = RatingVectorUserHistorySummarizer.makeRatingVector(evt).mutableCopy();
                // vector is now the user's rating vector
                // TODO Normalize this vector and store the ratings in the item data
                
                double mean = vector.mean();
                Iterator<VectorEntry>iterator = vector.iterator();
                
                // get vector entries for the items that this user has rated
                while(iterator.hasNext()) {
                	
                	VectorEntry entry = iterator.next();
                	Map<Long, Double> mapForItem = itemData.get(entry.getKey());
                	double rating = entry.getValue();
                	
                	// normalize and store them in itemData
                	mapForItem.put(evt.getUserId(), rating - mean);
                }
            }
        } finally {
            stream.close();
        }

        // This loop converts our temporary item storage to a map of item vectors
        Map<Long,ImmutableSparseVector> itemVectors = new HashMap<Long, ImmutableSparseVector>();
        for (Map.Entry<Long,Map<Long,Double>> entry: itemData.entrySet()) {
            MutableSparseVector vec = MutableSparseVector.create(entry.getValue());
            itemVectors.put(entry.getKey(), vec.immutable());
        }
        return itemVectors;
    }
}
