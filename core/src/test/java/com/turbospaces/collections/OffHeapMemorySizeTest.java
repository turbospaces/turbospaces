package com.turbospaces.collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import net.sf.ehcache.pool.Size;
import net.sf.ehcache.pool.sizeof.UnsafeSizeOf;

import org.junit.Test;
import org.springframework.data.mapping.model.BasicPersistentEntity;

import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.Memory;
import com.turbospaces.model.BO;
import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class OffHeapMemorySizeTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void allocateSegmentAndSet()
                                       throws Exception {
        SpaceConfiguration configuration = TestEntity1.configurationFor();
        BO bo = new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity( TestEntity1.class ) );
        UnsafeSizeOf sizeOf = new UnsafeSizeOf();
        OffHeapLinearProbingSet set = new OffHeapLinearProbingSet( configuration, bo );
        Size setSizeOf = sizeOf.deepSizeOf( Integer.MAX_VALUE, true, set );
        assertThat( Memory.toMb( setSizeOf.getCalculated() ), isOneOf( 3, 4 ) );
    }
}
