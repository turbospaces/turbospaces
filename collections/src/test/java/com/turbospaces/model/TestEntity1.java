/**
 * Copyright (C) 2011 Andrey Borisov <aandrey.borisov@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.turbospaces.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.beans.IntrospectionException;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Routing;
import org.springframework.data.annotation.Version;
import org.springframework.data.mapping.model.BasicPersistentEntity;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

@SuppressWarnings("javadoc")
public class TestEntity1 implements InitializingBean, Cloneable {
    private static final Random random = new Random();

    public String s1, s2, s3, s4;
    public Long l1, l2, l3, l4;
    public Date dt1, dt2, dt3, dt4;
    public Double d1, d2, d3, d4;
    public Float f1, f2, f3, f4;
    public Integer fin1, fin2;

    public boolean b1, b2, b3, b4;
    public short sh1, sh2, sh3, sh4;
    public double dp1, dp2, dp3, dp4;
    public float fp1, fp2, fp3, fp4;
    public char cp1, cp2, cp3, cp4;
    public int fi1, fi2;
    public long lp1, lp2;

    @Id
    public String uniqueIdentifier;
    @Version
    public Integer optimisticLockVersion;
    @Routing
    public String routing;

    public Object data1, data2;

    public RoundingMode mode;
    public RoundingMode[] roundingModes;
    public Autowire autowire;
    public ArrayList<Long> longs;

    public TestEntity1() {}

    @Override
    public void afterPropertiesSet() {
        afterPropertiesSet( getClass().getSimpleName() + ":" + UUID.randomUUID().toString() );
    }

    public void afterPropertiesSet(final String id) {
        s1 = "s1" + random.nextDouble();
        s2 = "s2" + random.nextDouble();
        s3 = "s3" + random.nextDouble();
        s4 = "s4" + random.nextDouble();

        l1 = random.nextLong();
        l2 = random.nextLong();
        l3 = random.nextLong();
        l4 = random.nextLong();

        dt1 = new Date( System.currentTimeMillis() - 1000 );
        dt2 = new Date( System.currentTimeMillis() + 1000 );
        dt3 = new Date( dt1.getTime() );
        dt4 = new Date( dt2.getTime() );

        d1 = random.nextDouble();
        d2 = random.nextDouble();
        d3 = random.nextDouble();
        d4 = random.nextDouble();

        f1 = random.nextFloat();
        f2 = random.nextFloat();
        f3 = random.nextFloat();
        f4 = random.nextFloat();

        fi1 = random.nextInt();
        fi2 = random.nextInt();

        data1 = Integer.valueOf( random.nextInt() );
        uniqueIdentifier = id;
        mode = RoundingMode.FLOOR;
        autowire = Autowire.BY_TYPE;
        routing = "hash" + random.nextLong();
        roundingModes = RoundingMode.values();
        longs = Lists.newArrayList();
        longs.add( Long.valueOf( 1L ) );
        longs.add( Long.valueOf( 2L ) );
        longs.add( Long.valueOf( 3L ) );

        b1 = random.nextBoolean();
        b2 = random.nextBoolean();
        b3 = random.nextBoolean();
        b4 = random.nextBoolean();

        sh1 = (short) random.nextInt();
        sh2 = (short) random.nextInt();
        sh3 = (short) random.nextInt();
        sh4 = (short) random.nextInt();

        dp1 = random.nextDouble();
        dp2 = random.nextDouble();
        dp3 = random.nextDouble();
        dp4 = random.nextDouble();

        fp1 = random.nextFloat();
        fp2 = random.nextFloat();
        fp3 = random.nextFloat();
        fp4 = random.nextFloat();

        cp1 = (char) random.nextInt();
        cp2 = (char) random.nextInt();
        cp3 = (char) random.nextInt();
        cp4 = (char) random.nextInt();

        lp1 = random.nextLong();
        lp2 = random.nextLong();
    }

    public RoundingMode getMode() {
        return mode;
    }

    public void setMode(final RoundingMode mode) {
        this.mode = mode;
    }

    public RoundingMode[] getRoundingModes() {
        return roundingModes;
    }

    public void setRoundingModes(final RoundingMode[] roundingModes) {
        this.roundingModes = roundingModes;
    }

    public Autowire getAutowire() {
        return autowire;
    }

    public void setAutowire(final Autowire autowire) {
        this.autowire = autowire;
    }

    public ArrayList<Long> getLongs() {
        return longs;
    }

    public void setLongs(final ArrayList<Long> longs) {
        this.longs = longs;
    }

    public String getUniqueIdentifier() {
        return uniqueIdentifier;
    }

    public void setUniqueIdentifier(final String uniqueIdentifier) {
        this.uniqueIdentifier = uniqueIdentifier;
    }

    public void setOptimisticLockVersion(final Integer version) {
        this.optimisticLockVersion = version;
    }

    public Integer getOptimisticLockVersion() {
        return optimisticLockVersion;
    }

    public void cleanBeanProperties() {
        uniqueIdentifier = null;
        optimisticLockVersion = null;

        s1 = null;
        s2 = null;
        s3 = null;
        s4 = null;

        l1 = null;
        l2 = null;
        l3 = null;
        l4 = null;

        dt1 = null;
        dt2 = null;
        dt3 = null;
        dt4 = null;

        d1 = null;
        d2 = null;
        d3 = null;
        d4 = null;

        f1 = null;
        f2 = null;
        f3 = null;
        f4 = null;

        fi1 = 0;
        fi2 = 0;

        fin1 = null;
        fin2 = null;
        data1 = null;
        data2 = null;
        mode = null;
        roundingModes = null;
        autowire = null;
        routing = null;
        longs = null;

        b1 = false;
        b2 = false;
        b3 = false;
        b4 = false;

        sh1 = 0;
        sh2 = 0;
        sh3 = 0;
        sh4 = 0;

        dp1 = 0.0d;
        dp2 = 0.0d;
        dp3 = 0.0d;
        dp4 = 0.0d;

        fp1 = 0.0f;
        fp2 = 0.0f;
        fp3 = 0.0f;
        fp4 = 0.0f;

        cp1 = 0;
        cp2 = 0;
        cp3 = 0;
        cp4 = 0;

        lp1 = 0;
        lp2 = 0;
    }

    @Override
    public TestEntity1 clone() {
        TestEntity1 e1 = new TestEntity1();
        e1.s1 = s1;
        e1.s2 = s2;
        e1.s3 = s3;
        e1.s4 = s4;

        e1.l1 = l1;
        e1.l2 = l2;
        e1.l3 = l4;
        e1.l4 = l4;

        e1.dt1 = dt1;
        e1.dt2 = dt2;
        e1.dt3 = dt3;
        e1.dt4 = dt4;

        e1.d1 = d1;
        e1.d2 = d2;
        e1.d3 = d3;
        e1.d4 = d4;

        e1.f1 = f1;
        e1.f2 = f2;
        e1.f3 = f3;
        e1.f4 = f4;

        e1.fi1 = fi1;
        e1.fi2 = fi2;
        e1.fin1 = fin1;
        e1.fin2 = fin2;
        e1.uniqueIdentifier = uniqueIdentifier;
        e1.optimisticLockVersion = optimisticLockVersion;
        e1.routing = routing;

        e1.data1 = data1;
        e1.data2 = data2;

        e1.mode = mode;
        e1.roundingModes = roundingModes;
        e1.autowire = autowire;
        e1.longs = longs;

        e1.b1 = b1;
        e1.b2 = b2;
        e1.b3 = b3;
        e1.b4 = b4;

        e1.sh1 = sh1;
        e1.sh2 = sh2;
        e1.sh3 = sh3;
        e1.sh4 = sh4;

        e1.dp1 = dp1;
        e1.dp2 = dp2;
        e1.dp3 = dp3;
        e1.dp4 = dp4;

        e1.dp1 = fp1;
        e1.fp2 = fp2;
        e1.fp3 = fp3;
        e1.fp4 = fp4;

        e1.cp1 = cp1;
        e1.cp2 = cp2;
        e1.cp3 = cp3;
        e1.cp4 = cp4;

        e1.lp1 = lp1;
        e1.lp2 = lp2;

        return e1;
    }

    public void assertMatch(final TestEntity1 data2) {
        assertThat( data2.getS1(), is( getS1() ) );
        assertThat( data2.getS4(), is( getS4() ) );
        assertThat( data2.getL1(), is( getL1() ) );
        assertThat( data2.getL4(), is( getL4() ) );
        assertThat( data2.getDt1(), is( getDt1() ) );
        assertThat( data2.getDt4(), is( getDt4() ) );
        assertThat( data2.getD1(), is( getD1() ) );
        assertThat( data2.getD4(), is( getD4() ) );
        assertThat( data2.getF1(), is( getF1() ) );
        assertThat( data2.getF4(), is( getF4() ) );
        assertThat( data2.getFi1(), is( getFi1() ) );
        assertThat( data2.getFi2(), is( getFi2() ) );
        assertThat( data2.getFin1(), is( nullValue() ) );
        assertThat( data2.getFin2(), is( nullValue() ) );
        assertThat( data2.getUniqueIdentifier(), is( getUniqueIdentifier() ) );
        assertThat( data2.getOptimisticLockVersion(), is( getOptimisticLockVersion() ) );
        assertThat( data2.getMode(), is( getMode() ) );
        assertThat( data2.getAutowire(), is( getAutowire() ) );
        assertThat( data2.getRoundingModes(), is( getRoundingModes() ) );
        assertThat( data2.data1, is( data1 ) );
        assertThat( data2.routing, is( routing ) );
        assertThat( data2.data2, is( nullValue() ) );
        assertThat( data2.data2, is( nullValue() ) );
        assertThat( data2.getLongs().size(), is( 3 ) );
        assertThat( data2.getB1(), is( getB1() ) );
        assertThat( data2.getB2(), is( getB2() ) );
        assertThat( data2.getB3(), is( getB3() ) );
        assertThat( data2.getB4(), is( getB4() ) );
        assertThat( data2.getSh1(), is( getSh1() ) );
        assertThat( data2.getSh2(), is( getSh2() ) );
        assertThat( data2.getSh3(), is( getSh3() ) );
        assertThat( data2.getSh4(), is( getSh4() ) );
        assertThat( data2.getDp1(), is( getDp1() ) );
        assertThat( data2.getDp2(), is( getDp2() ) );
        assertThat( data2.getDp3(), is( getDp3() ) );
        assertThat( data2.getDp4(), is( getDp4() ) );
        assertThat( data2.getFp1(), is( getFp1() ) );
        assertThat( data2.getFp2(), is( getFp2() ) );
        assertThat( data2.getFp3(), is( getFp3() ) );
        assertThat( data2.getFp4(), is( getFp4() ) );
        assertThat( data2.getCp1(), is( getCp1() ) );
        assertThat( data2.getCp2(), is( getCp2() ) );
        assertThat( data2.getCp3(), is( getCp3() ) );
        assertThat( data2.getCp4(), is( getCp4() ) );
        assertThat( data2.getLp1(), is( getLp1() ) );
        assertThat( data2.getLp2(), is( getLp2() ) );
    }

    public long getLp1() {
        return lp1;
    }

    public void setLp1(final long lp1) {
        this.lp1 = lp1;
    }

    public long getLp2() {
        return lp2;
    }

    public void setLp2(final long lp2) {
        this.lp2 = lp2;
    }

    public char getCp1() {
        return cp1;
    }

    public void setCp1(final char cp1) {
        this.cp1 = cp1;
    }

    public char getCp2() {
        return cp2;
    }

    public void setCp2(final char cp2) {
        this.cp2 = cp2;
    }

    public char getCp3() {
        return cp3;
    }

    public void setCp3(final char cp3) {
        this.cp3 = cp3;
    }

    public char getCp4() {
        return cp4;
    }

    public void setCp4(final char cp4) {
        this.cp4 = cp4;
    }

    public float getFp1() {
        return fp1;
    }

    public void setFp1(final float fp1) {
        this.fp1 = fp1;
    }

    public float getFp2() {
        return fp2;
    }

    public void setFp2(final float fp2) {
        this.fp2 = fp2;
    }

    public float getFp3() {
        return fp3;
    }

    public void setFp3(final float fp3) {
        this.fp3 = fp3;
    }

    public float getFp4() {
        return fp4;
    }

    public void setFp4(final float fp4) {
        this.fp4 = fp4;
    }

    public double getDp1() {
        return dp1;
    }

    public void setDp1(final double dp1) {
        this.dp1 = dp1;
    }

    public double getDp2() {
        return dp2;
    }

    public void setDp2(final double dp2) {
        this.dp2 = dp2;
    }

    public double getDp3() {
        return dp3;
    }

    public void setDp3(final double dp3) {
        this.dp3 = dp3;
    }

    public double getDp4() {
        return dp4;
    }

    public void setDp4(final double dp4) {
        this.dp4 = dp4;
    }

    public String getS1() {
        return s1;
    }

    public void setS1(final String s1) {
        this.s1 = s1;
    }

    public String getS2() {
        return s2;
    }

    public void setS2(final String s2) {
        this.s2 = s2;
    }

    public String getS3() {
        return s3;
    }

    public void setS3(final String s3) {
        this.s3 = s3;
    }

    public String getS4() {
        return s4;
    }

    public void setS4(final String s4) {
        this.s4 = s4;
    }

    public Long getL1() {
        return l1;
    }

    public void setL1(final Long l1) {
        this.l1 = l1;
    }

    public Long getL2() {
        return l2;
    }

    public void setL2(final Long l2) {
        this.l2 = l2;
    }

    public Long getL3() {
        return l3;
    }

    public void setL3(final Long l3) {
        this.l3 = l3;
    }

    public Long getL4() {
        return l4;
    }

    public void setL4(final Long l4) {
        this.l4 = l4;
    }

    public Date getDt1() {
        return dt1;
    }

    public void setDt1(final Date dt1) {
        this.dt1 = dt1;
    }

    public Date getDt2() {
        return dt2;
    }

    public void setDt2(final Date dt2) {
        this.dt2 = dt2;
    }

    public Date getDt3() {
        return dt3;
    }

    public void setDt3(final Date dt3) {
        this.dt3 = dt3;
    }

    public Date getDt4() {
        return dt4;
    }

    public void setDt4(final Date dt4) {
        this.dt4 = dt4;
    }

    public Double getD1() {
        return d1;
    }

    public void setD1(final Double d1) {
        this.d1 = d1;
    }

    public Double getD2() {
        return d2;
    }

    public void setD2(final Double d2) {
        this.d2 = d2;
    }

    public Double getD3() {
        return d3;
    }

    public void setD3(final Double d3) {
        this.d3 = d3;
    }

    public Double getD4() {
        return d4;
    }

    public void setD4(final Double d4) {
        this.d4 = d4;
    }

    public Float getF1() {
        return f1;
    }

    public void setF1(final Float f1) {
        this.f1 = f1;
    }

    public Float getF2() {
        return f2;
    }

    public void setF2(final Float f2) {
        this.f2 = f2;
    }

    public Float getF3() {
        return f3;
    }

    public void setF3(final Float f3) {
        this.f3 = f3;
    }

    public Float getF4() {
        return f4;
    }

    public void setF4(final Float f4) {
        this.f4 = f4;
    }

    public int getFi1() {
        return fi1;
    }

    public void setFi1(final int fi1) {
        this.fi1 = fi1;
    }

    public int getFi2() {
        return fi2;
    }

    public void setFi2(final int fi2) {
        this.fi2 = fi2;
    }

    public Integer getFin1() {
        return fin1;
    }

    public void setFin1(final Integer fin1) {
        this.fin1 = fin1;
    }

    public Integer getFin2() {
        return fin2;
    }

    public void setFin2(final Integer fin2) {
        this.fin2 = fin2;
    }

    public Object getData1() {
        return data1;
    }

    public void setData1(final Object data1) {
        this.data1 = data1;
    }

    public Object getData2() {
        return data2;
    }

    public void setData2(final Object data2) {
        this.data2 = data2;
    }

    public String getRouting() {
        return routing;
    }

    public void setRouting(final String routing) {
        this.routing = routing;
    }

    public boolean getB1() {
        return b1;
    }

    public void setB1(final boolean b1) {
        this.b1 = b1;
    }

    public boolean getB2() {
        return b2;
    }

    public void setB2(final boolean b2) {
        this.b2 = b2;
    }

    public boolean getB3() {
        return b3;
    }

    public void setB3(final boolean b3) {
        this.b3 = b3;
    }

    public boolean getB4() {
        return b4;
    }

    public void setB4(final boolean b4) {
        this.b4 = b4;
    }

    public short getSh1() {
        return sh1;
    }

    public void setSh1(final short sh1) {
        this.sh1 = sh1;
    }

    public short getSh2() {
        return sh2;
    }

    public void setSh2(final short sh2) {
        this.sh2 = sh2;
    }

    public short getSh3() {
        return sh3;
    }

    public void setSh3(final short sh3) {
        this.sh3 = sh3;
    }

    public short getSh4() {
        return sh4;
    }

    public void setSh4(final short sh4) {
        this.sh4 = sh4;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode( getUniqueIdentifier(), getOptimisticLockVersion(), getRouting() );
    }

    @Override
    public boolean equals(final Object obj) {
        if ( obj instanceof TestEntity1 ) {
            TestEntity1 other = (TestEntity1) obj;
            return Objects.equal( other.getUniqueIdentifier(), getUniqueIdentifier() )
                    && Objects.equal( other.getOptimisticLockVersion(), getOptimisticLockVersion() )
                    && Objects.equal( other.getRouting(), getRouting() );
        }
        return super.equals( obj );
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper( this )
                .add( "id", getUniqueIdentifier() )
                .add( "version", getOptimisticLockVersion() )
                .add( "routing", getRouting() )
                .toString();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static BO getPersistentEntity()
                                          throws SecurityException,
                                          NoSuchMethodException,
                                          IntrospectionException {
        SimpleMappingContext mappingContext = new SimpleMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity1.class ) );
        mappingContext.afterPropertiesSet();
        return new BO( (BasicPersistentEntity) mappingContext.getPersistentEntity( TestEntity1.class ) );
    }
}
