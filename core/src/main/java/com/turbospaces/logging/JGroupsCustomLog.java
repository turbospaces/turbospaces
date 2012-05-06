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
package com.turbospaces.logging;

import org.jgroups.logging.Log;
import org.slf4j.Logger;
import org.slf4j.Marker;

class JGroupsCustomLog implements Log {
    private final Logger logger;

    public JGroupsCustomLog(final Logger logger) {
        this.logger = logger;
    }

    @Override
    public boolean isFatalEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    public String getName() {
        return logger.getName();
    }

    @Override
    public void trace(final String msg) {
        logger.trace( msg );
    }

    public void trace(final String format,
                      final Object arg) {
        logger.trace( format, arg );
    }

    public void trace(final String format,
                      final Object arg1,
                      final Object arg2) {
        logger.trace( format, arg1, arg2 );
    }

    public void trace(final String format,
                      final Object[] argArray) {
        logger.trace( format, argArray );
    }

    @Override
    public void trace(final String msg,
                      final Throwable t) {
        logger.trace( msg, t );
    }

    public boolean isTraceEnabled(final Marker marker) {
        return logger.isTraceEnabled( marker );
    }

    public void trace(final Marker marker,
                      final String msg) {
        logger.trace( marker, msg );
    }

    public void trace(final Marker marker,
                      final String format,
                      final Object arg) {
        logger.trace( marker, format, arg );
    }

    public void trace(final Marker marker,
                      final String format,
                      final Object arg1,
                      final Object arg2) {
        logger.trace( marker, format, arg1, arg2 );
    }

    public void trace(final Marker marker,
                      final String format,
                      final Object[] argArray) {
        logger.trace( marker, format, argArray );
    }

    public void trace(final Marker marker,
                      final String msg,
                      final Throwable t) {
        logger.trace( marker, msg, t );
    }

    @Override
    public void debug(final String msg) {
        logger.debug( msg );
    }

    public void debug(final String format,
                      final Object arg) {
        logger.debug( format, arg );
    }

    public void debug(final String format,
                      final Object arg1,
                      final Object arg2) {
        logger.debug( format, arg1, arg2 );
    }

    public void debug(final String format,
                      final Object[] argArray) {
        logger.debug( format, argArray );
    }

    @Override
    public void debug(final String msg,
                      final Throwable t) {
        logger.debug( msg, t );
    }

    public boolean isDebugEnabled(final Marker marker) {
        return logger.isDebugEnabled( marker );
    }

    public void debug(final Marker marker,
                      final String msg) {
        logger.debug( marker, msg );
    }

    public void debug(final Marker marker,
                      final String format,
                      final Object arg) {
        logger.debug( marker, format, arg );
    }

    public void debug(final Marker marker,
                      final String format,
                      final Object arg1,
                      final Object arg2) {
        logger.debug( marker, format, arg1, arg2 );
    }

    public void debug(final Marker marker,
                      final String format,
                      final Object[] argArray) {
        logger.debug( marker, format, argArray );
    }

    public void debug(final Marker marker,
                      final String msg,
                      final Throwable t) {
        logger.debug( marker, msg, t );
    }

    @Override
    public void info(final String msg) {
        logger.info( msg );
    }

    public void info(final String format,
                     final Object arg) {
        logger.info( format, arg );
    }

    public void info(final String format,
                     final Object arg1,
                     final Object arg2) {
        logger.info( format, arg1, arg2 );
    }

    public void info(final String format,
                     final Object[] argArray) {
        logger.info( format, argArray );
    }

    public void info(final String msg,
                     final Throwable t) {
        logger.info( msg, t );
    }

    public boolean isInfoEnabled(final Marker marker) {
        return logger.isInfoEnabled( marker );
    }

    public void info(final Marker marker,
                     final String msg) {
        logger.info( marker, msg );
    }

    public void info(final Marker marker,
                     final String format,
                     final Object arg) {
        logger.info( marker, format, arg );
    }

    public void info(final Marker marker,
                     final String format,
                     final Object arg1,
                     final Object arg2) {
        logger.info( marker, format, arg1, arg2 );
    }

    public void info(final Marker marker,
                     final String format,
                     final Object[] argArray) {
        logger.info( marker, format, argArray );
    }

    public void info(final Marker marker,
                     final String msg,
                     final Throwable t) {
        logger.info( marker, msg, t );
    }

    @Override
    public void warn(final String msg) {
        logger.warn( msg );
    }

    public void warn(final String format,
                     final Object arg) {
        logger.warn( format, arg );
    }

    public void warn(final String format,
                     final Object[] argArray) {
        logger.warn( format, argArray );
    }

    public void warn(final String format,
                     final Object arg1,
                     final Object arg2) {
        logger.warn( format, arg1, arg2 );
    }

    @Override
    public void warn(final String msg,
                     final Throwable t) {
        logger.warn( msg, t );
    }

    public boolean isWarnEnabled(final Marker marker) {
        return logger.isWarnEnabled( marker );
    }

    public void warn(final Marker marker,
                     final String msg) {
        logger.warn( marker, msg );
    }

    public void warn(final Marker marker,
                     final String format,
                     final Object arg) {
        logger.warn( marker, format, arg );
    }

    public void warn(final Marker marker,
                     final String format,
                     final Object arg1,
                     final Object arg2) {
        logger.warn( marker, format, arg1, arg2 );
    }

    public void warn(final Marker marker,
                     final String format,
                     final Object[] argArray) {
        logger.warn( marker, format, argArray );
    }

    public void warn(final Marker marker,
                     final String msg,
                     final Throwable t) {
        logger.warn( marker, msg, t );
    }

    @Override
    public void error(final String msg) {
        logger.error( msg );
    }

    public void error(final String format,
                      final Object arg) {
        logger.error( format, arg );
    }

    public void error(final String format,
                      final Object arg1,
                      final Object arg2) {
        logger.error( format, arg1, arg2 );
    }

    public void error(final String format,
                      final Object[] argArray) {
        logger.error( format, argArray );
    }

    @Override
    public void error(final String msg,
                      final Throwable t) {
        logger.error( msg, t );
    }

    public boolean isErrorEnabled(final Marker marker) {
        return logger.isErrorEnabled( marker );
    }

    public void error(final Marker marker,
                      final String msg) {
        logger.error( marker, msg );
    }

    public void error(final Marker marker,
                      final String format,
                      final Object arg) {
        logger.error( marker, format, arg );
    }

    public void error(final Marker marker,
                      final String format,
                      final Object arg1,
                      final Object arg2) {
        logger.error( marker, format, arg1, arg2 );
    }

    public void error(final Marker marker,
                      final String format,
                      final Object[] argArray) {
        logger.error( marker, format, argArray );
    }

    public void error(final Marker marker,
                      final String msg,
                      final Throwable t) {
        logger.error( marker, msg, t );
    }

    @Override
    public void fatal(final String msg) {
        logger.error( msg );
    }

    @Override
    public void fatal(final String msg,
                      final Throwable throwable) {
        logger.error( msg, throwable );
    }

    @Override
    public void trace(final Object msg) {
        logger.trace( msg.toString() );
    }

    @Override
    public void setLevel(final String level) {}

    @Override
    public String getLevel() {
        return "INFO";
    }
}
