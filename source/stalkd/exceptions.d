// Copyright (c) 2013, Peter Wood.
// See license.txt for licensing details.
module stalkd.exceptions;

/**
 * This class provides the exception class used by the library.
 */
class StalkdException : Exception {
   this(string message, Throwable thrown=null) {
      super(message, thrown);
   }
}
